#include "w_defines.h"

#include "w.h"
#include "os_types.h"
#include "os_fcntl.h"

#include "sthread.h"
#include "sthread_stats.h"
#include "sthread_vtable_enum.h"
#include "w_strstream.h"

#include <iostream>
#include "gtest/gtest.h"

class io_thread_t : public sthread_t {
public:
        io_thread_t(int i, char *bf);

protected:
        void    run();

private:
        int     idx;
        char    *buf;
};


class cpu_thread_t : public sthread_t {
public:
        cpu_thread_t(int i);

protected:
        void    run();

private:
        int     idx;
};



io_thread_t     **ioThread;
cpu_thread_t    **cpuThread;

int     NumIOs = 30;
int     NumThreads = 5;
int     BufSize = 1024;
int     vec_size = 0;           // i/o vector slots for an i/o operation
bool    raw_io = false;
bool    sync_io = true;
bool    histograms = false;
bool    verbose = false;

/* build an i/o vector for an I/O operation, to test write buffer. */
int make_vec(char *buf, int size, int vec_size,
                sthread_t::iovec_t *vec, const int iovec_max)
{
        int     slot = 0;

        /* Default I/O vector size is the entire i/o */
        if (vec_size == 0)
                vec_size = size;

        while (size > vec_size && slot < iovec_max-1) {
                vec[slot].iov_len = vec_size;
                vec[slot].iov_base = buf;
                buf += vec_size;
                size -= vec_size;
                slot++;
        }
        if (size) {
                vec[slot].iov_len = size;
                vec[slot].iov_base = buf;
                slot++;
        }

        return slot;
}



io_thread_t::io_thread_t(int i, char *bf)
:
  sthread_t(t_regular),
  idx(i),
  buf(bf)
{
        w_ostrstream_buf s(40);         // XXX magic number

        s << "io[" << idx << "]" << ends;
        rename(s.c_str());
}


void io_thread_t::run()
{
        cout << name() << ": started: "
                << NumIOs << " I/Os "
                << endl;

        w_ostrstream_buf f(40); /* XXX sb maxpathlen? */

        f << "./sthread." << getpid() << '.' << idx << ends;
        const char *fname = f.c_str();

        int fd;
        w_rc_t rc;
        int flags = OPEN_RDWR | OPEN_CREATE;

        if (raw_io)
                flags |= OPEN_DIRECT;
        if (sync_io)
                flags |= OPEN_SYNC;

        rc = sthread_t::open(fname, flags, 0666, fd);
        EXPECT_FALSE(rc.is_error())
            << "open " << fname << ":" << endl << rc;

        int i;
        for (i = 0; i < NumIOs; i++)  {
                sthread_t::iovec_t      vec[iovec_max]; /*XXX magic number */
                int     cnt;
                for (register int j = 0; j < BufSize; j++)
                        buf[j] = (unsigned char) i;

                cnt = make_vec(buf, BufSize, vec_size, vec, iovec_max);

                // cout  << name() << endl ;
                rc = sthread_t::writev(fd, vec, cnt);
                EXPECT_FALSE(rc.is_error())
                        << "write:" << endl << rc;
        }

        // cout << name() << ": finished writing" << endl;

        rc = sthread_t::lseek(fd, 0, SEEK_AT_SET);
        EXPECT_FALSE(rc.is_error())
                << "lseek:" << endl << rc;

        // cout << name() << ": finished seeking" << endl;
        for (i = 0; i < NumIOs; i++) {
                rc = sthread_t::read(fd, buf, BufSize);
                EXPECT_FALSE (rc.is_error())
                        << "read:" << endl << rc;
                for (register int j = 0; j < BufSize; j++) {
                    EXPECT_EQ((unsigned char)buf[j], (unsigned char)i)
                            << name() << ": read bad data"
                            << " (page " << i
                            << "  expected " << ((int)j)
                            << " got " << buf[j];
                }
        }
        // cout << "'" << name() << "': finished reading" << endl;

        EXPECT_FALSE( sthread_t::fsync(fd).is_error() );

        EXPECT_FALSE( sthread_t::close(fd).is_error() );

        // cleanup after ourself
        (void) ::unlink(fname);
}

void print_histograms(ostream &o)
{
        static vtable_t  t;
        if (sthread_t::collect(t, true)<0)
                o << "THREADS: error" << endl;
        else {
                o << "THREAD " <<endl;
                t.operator<<(o);
        }

        o << "-----------------" << endl;

}


void runtest() {
#ifdef HAVE_HUGETLBFS
    EXPECT_FALSE(sthread_t::set_hugetlbfs_path(HUGETLBFS_PATH).is_error());
#endif
    char        *buf;
    sthread_t::set_bufsize(BufSize * NumThreads, buf);
    EXPECT_TRUE(buf != 0);

    cout << "Num Threads = " << NumThreads << endl;

    ioThread = new io_thread_t *[NumThreads];
    EXPECT_TRUE (ioThread != NULL);

    int i;
    for (i = 0; i < NumThreads; i++)  {
        ioThread[i] = new io_thread_t(i, buf + i*BufSize);
        EXPECT_TRUE (ioThread[i] != NULL);
    }

    /* Start them after they are allocated, so we get some parallelism */
    for (i = 0; i < NumThreads; i++)
        EXPECT_FALSE(ioThread[i]->fork().is_error());

        // Do this while some threads are around.
    if (histograms)
        print_histograms(cout);

    for (i = 0; i < NumThreads; i++)
        EXPECT_FALSE( ioThread[i]->join().is_error() );


    for (i = 0; i < NumThreads; i++) delete ioThread[i];
    delete [] ioThread;
    sthread_t::dump_stats(cout);
    EXPECT_FALSE(sthread_t::set_bufsize(0, buf).is_error());
}

TEST (ThreadTest2, NoHistogram) {
    histograms = false;
    runtest();
}
TEST (ThreadTest2, Histogram) {
    histograms = true;
    runtest();
}
