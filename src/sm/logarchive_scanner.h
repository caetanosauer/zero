#ifndef LOGARCHIVE_SCANNER_H
#define LOGARCHIVE_SCANNER_H

#include <iostream>
#include <memory>
#include <vector>

#include "basics.h"
#include "lsn.h"
#include "logarchive_index.h"

class ArchiveIndex;
class RunFile;
class logrec_t;

struct MergeInput
{
    RunFile* runFile;
    size_t pos;
    lsn_t keyLSN;
    PageID keyPID;
    PageID endPID;


    logrec_t* logrec();
    bool open(PageID startPID);
    bool finished();
    void next();

    friend bool mergeInputCmpGt(const MergeInput& a, const MergeInput& b);
};


// Merge input should be exactly 1/2 of a cacheline
static_assert(sizeof(MergeInput) == 32, "Misaligned MergeInput");

class ArchiveScan {
public:
    ArchiveScan(std::shared_ptr<ArchiveIndex>);
    ~ArchiveScan();

    void open(PageID startPID, PageID endPID, lsn_t startLSN,
            lsn_t endLSN = lsn_t::null);
    bool next(logrec_t*&);
    bool finished();

    template <class Iter>
    void openForMerge(Iter begin, Iter end);

    void dumpHeap();

private:
    // Thread-local storage for merge inputs
    static thread_local std::vector<MergeInput> _mergeInputVector;

    std::vector<MergeInput>::iterator heapBegin;
    std::vector<MergeInput>::iterator heapEnd;

    std::shared_ptr<ArchiveIndex> archIndex;
    lsn_t prevLSN;
    PageID prevPID;
    bool singlePage;

    void clear();
};

bool mergeInputCmpGt(const MergeInput& a, const MergeInput& b);

template <class Iter>
void ArchiveScan::openForMerge(Iter begin, Iter end)
{
    w_assert0(archIndex);
    clear();
    auto& inputs = _mergeInputVector;

    for (Iter it = begin; it != end; it++) {
        MergeInput input;
        auto runid = *it;
        input.pos = 0;
        input.runFile = archIndex->openForScan(*it);
        inputs.push_back(input);
    }

    heapBegin = inputs.begin();
    auto it = inputs.rbegin();
    while (it != inputs.rend())
    {
        constexpr PageID startPID = 0;
        if (it->open(startPID)) { it++; }
        else {
            std::advance(it, 1);
            inputs.erase(it.base());
        }
    }

    heapEnd = inputs.end();
    std::make_heap(heapBegin, heapEnd, mergeInputCmpGt);
}

#endif
