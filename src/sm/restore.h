#include "w_defines.h"

#ifndef RESTORE_H
#define RESTORE_H

#include "worker_thread.h"
#include "sm_base.h"
#include "logarchive_scanner.h"

#include <queue>
#include <map>
#include <vector>
#include <unordered_map>
#include <atomic>
#include <mutex>
#include <chrono>
#include <condition_variable>

class sm_options;
class RestoreBitmap;
class ArchiveIndex;

/** \brief Bitmap data structure that controls the progress of restore
 *
 * The bitmap contains one bit for each segment of the failed volume.  All bits
 * are initially "false", and a bit is set to "true" when the corresponding
 * segment has been restored. This class is completely oblivious to pages
 * inside a segment -- it is the callers resposibility to interpret what a
 * segment consists of.
 */
class RestoreBitmap {
public:

    enum class State {
        UNRESTORED = 0,
        RESTORING = 1,
        RESTORED = 2
    };

    RestoreBitmap(size_t size)
        : _size(size)
    {
        states = new std::atomic<State>[size];
        for (size_t i = 0; i < size; i++) {
            states[i] = State::UNRESTORED;
        }
    }

    ~RestoreBitmap()
    {
        delete[] states;
    }

    size_t get_size() { return _size; }


    bool is_unrestored(unsigned i) const
    {
        return states[i] == State::UNRESTORED;
    }

    bool is_restoring(unsigned i) const
    {
        return states[i] == State::RESTORING;
    }

    bool is_restored(unsigned i) const
    {
        return states[i] == State::RESTORED;
    }

    bool attempt_restore(unsigned i)
    {
        auto expected = State::UNRESTORED;
        return states[i].compare_exchange_strong(expected, State::RESTORING);
    }

    void mark_restored(unsigned i)
    {
        w_assert1(states[i] == State::RESTORING);
        states[i] = State::RESTORED;
    }

    unsigned get_first_unrestored() const
    {
        for (unsigned i = 0; i < _size; i++) {
            if (states[i] == State::UNRESTORED) { return i; }
        }
        return _size;
    }

    unsigned get_first_restoring() const
    {
        for (unsigned i = 0; i < _size; i++) {
            if (states[i] == State::RESTORING) { return i; }
        }
        return _size;
    }

    // TODO: implement these to checkpoint bitmap state
    // void serialize(char* buf, size_t from, size_t to);
    // void deserialize(char* buf, size_t from, size_t to);

    /** Get lowest false value and highest true value in order to compress
     * serialized format. Such compression is effective in a single-pass or
     * schedule. It is basically a run-length encoding, but supporting only a
     * run of ones in the beginning and a run of zeroes in the end of the
     * bitmap
     */
    // void getBoundaries(size_t& lowestFalse, size_t& highestTrue);

protected:
    std::atomic<State>* states;
    const size_t _size;
};

/** \brief Coordinator that synchronizes multi-threaded decentralized restore
 */
template <typename RestoreFunctor>
class RestoreCoordinator
{
public:

    RestoreCoordinator(size_t segSize, size_t segCount, RestoreFunctor f,
            bool virgin_pages, bool start_locked)
        : _segmentSize{segSize}, _bitmap{new RestoreBitmap {segCount}},
        _restoreFunctor{f}, _virgin_pages{virgin_pages}, _start_locked(start_locked)
    {
        if (_start_locked) {
            _mutex.lock();
        }
    }

    void fetch(PageID pid)
    {
        using namespace std::chrono_literals;

        auto segment = pid / _segmentSize;
        if (segment >= _bitmap->get_size() || _bitmap->is_restored(segment)) {
            return;
        }

        std::unique_lock<std::mutex> lck {_mutex};

        // check again in critical section
        if (_bitmap->is_restored(segment)) { return; }

        // Segment not restored yet: we must attempt to restore it ourselves or
        // wait on a ticket if it's already being restored
        auto ticket = getWaitingTicket(segment);

        if (_bitmap->attempt_restore(segment)) {
            lck.unlock();
            doRestore(segment, ticket);
        }
        else {
            auto pred = [this, segment] { return _bitmap->is_restored(segment); };
            while (!pred()) { ticket->wait_for(lck, 1ms, pred); }
        }
    }

    bool tryBackgroundRestore(bool& done)
    {
        done = false;

        // If no restore requests are pending, restore the first
        // not-yet-restored segment.
        if (!_waiting_table.empty()) { return false; }

        std::unique_lock<std::mutex> lck {_mutex};
        auto segment = _bitmap->get_first_unrestored();

        if (segment == _bitmap->get_size()) {
            // All segments in either "restoring" or "restored" state
            done = true;
            return false;
        }

        auto ticket = getWaitingTicket(segment);

        if (_bitmap->attempt_restore(segment)) {
            lck.unlock();
            doRestore(segment, ticket);
            return true;
        }

        return false;
    }

    bool isPidRestored(PageID pid) const
    {
        auto segment = pid / _segmentSize;
        return segment >= _bitmap->get_size() || _bitmap->is_restored(segment);
    }

    bool allDone() const
    {
        return _bitmap->get_first_restoring() >= _bitmap->get_size();
    }

    void start()
    {
        if (_start_locked) { _mutex.unlock(); }
    }

private:
    using Ticket = std::shared_ptr<std::condition_variable>;

    const size_t _segmentSize;

    std::mutex _mutex;
    std::unordered_map<unsigned, Ticket> _waiting_table;
    std::unique_ptr<RestoreBitmap> _bitmap;
    RestoreFunctor _restoreFunctor;
    const bool _virgin_pages;
    // This is used to make threads wait for log archiver reach a certain LSN
    const bool _start_locked;

    Ticket getWaitingTicket(unsigned segment)
    {
        auto it = _waiting_table.find(segment);
        if (it == _waiting_table.end()) {
            auto ticket = make_shared<std::condition_variable>();
            _waiting_table[segment] = ticket;
            return ticket;
        }
        else { return it->second; }
    }

    void doRestore(unsigned segment, Ticket ticket)
    {
        _restoreFunctor(segment, _segmentSize, _virgin_pages);

        _bitmap->mark_restored(segment);
        ticket->notify_all();

        std::unique_lock<std::mutex> lck {_mutex};
        _waiting_table.erase(segment);
    }
};

struct LogReplayer
{
    template <class LogScan, class PageIter>
    static void replay(LogScan logs, PageIter& pagesBegin, PageIter pagesEnd);
};

struct SegmentRestorer
{
    static void bf_restore(unsigned segment, size_t segmentSize, bool virgin_pages);
};

/** Thread that restores untouched segments in the background with low priority */
template <class Coordinator, class OnDoneCallback>
class BackgroundRestorer : public worker_thread_t
{
public:
    BackgroundRestorer(std::shared_ptr<Coordinator> coord, OnDoneCallback callback)
        : _coord(coord), _notify_done(callback)
    {
    }

    virtual void do_work()
    {
        bool no_segments_left = false;
        bool restored_last = false;

        auto do_sleep = [] {
            constexpr int sleep_time = 5;
            std::this_thread::sleep_for(std::chrono::seconds(sleep_time));
        };

        while (true) {
            if (!restored_last) { do_sleep(); }
            restored_last = _coord->tryBackgroundRestore(no_segments_left);

            if (no_segments_left || should_exit()) { break; }
        }

        while (!should_exit() && no_segments_left && !_coord->allDone()) {
            do_sleep();
        }

        if (_coord->allDone()) { _notify_done(); }

        _coord = nullptr;
        quit();
    }

private:
    std::shared_ptr<Coordinator> _coord;
    OnDoneCallback _notify_done;
};

#endif
