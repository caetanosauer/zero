#include "iterator.h"

#include <cassert>
#include <stdexcept>

const size_t PageIterator::PAGE_SIZE = 8192;

PageIterator::PageIterator(string inPath, string outPath,
        unsigned ioSizeInPages)
    : inPath(inPath), outPath(outPath), blockSize(ioSizeInPages * PAGE_SIZE),
    in(inPath), count(0), fpos(0), bpos(0), bytesRead(0), blocksRead(0),
    prevPageNo(0), asyncBuf(NULL)
{
    openOutput();
    buf = new char[ioSizeInPages * PAGE_SIZE];
    next(); // first page contains only system metadata
}

PageIterator::PageIterator(string inPath, string outPath,
        AsyncRingBuffer* asyncBuf, unsigned ioSizeInPages)
    : smthread_t(t_regular, "PageIterator"),
    inPath(inPath), outPath(outPath), blockSize(ioSizeInPages * PAGE_SIZE),
    in(inPath), count(0), fpos(0), bpos(0), bytesRead(0), blocksRead(0),
    buf(NULL), prevPageNo(0), asyncBuf(asyncBuf)
{
}

PageIterator::~PageIterator()
{
    if (!asyncBuf) delete buf;
    if (in.is_open()) {
        in.close();
    }
    if (out.is_open()) {
        out.close();
    }
}

void PageIterator::openOutput()
{
    if (!outPath.empty()) {
        out.exceptions (ofstream::failbit | ofstream::badbit);
        out.open(outPath, ios::binary | ios::out);
        out.clear();
        if (!out.is_open()) {
            throw runtime_error("Could not open output file");
        }
    }
}

void PageIterator::readBlock(char* b)
{
    if (out.is_open() && fpos > 0) {
        writeBlock(b);
    }
    in.seekg(fpos, ios::beg);
    in.read(b, blockSize);
    bytesRead = in.gcount();
    assert(bytesRead % 8192 == 0);
    if (in.eof()) {
        in.close();
    }
    fpos += bytesRead;
    blocksRead++;
}

void PageIterator::writeBlock(char* b)
{
    // seek beyond EOF only works with ios::end
    out.seekp((blocksRead -1 ) * blockSize, ios::beg);
    out.write(b, bytesRead);
    out.flush();
}

void PageIterator::writePage(char* buf, size_t index)
{
    out.seekp(index * PAGE_SIZE);
    out.write(buf, PAGE_SIZE);
}

bool PageIterator::hasNext()
{
    if (!asyncBuf) {
        return in.good() && in.is_open() &&
            (bpos < bytesRead || fpos == 0);
    }
    return bpos < bytesRead || !asyncBuf->isFinished();
}

generic_page* PageIterator::next()
{
    assert(hasNext());
    if (bpos == 0) {
        if (asyncBuf) {
            if(blocksRead > 0) {
                asyncBuf->consumerRelease();
            }
            buf = asyncBuf->consumerRequest();
        }
        else {
            readBlock(buf);
        }
    }
    if (asyncBuf && !hasNext()) {
        // consume request failed -> reader finished
        assert(!buf);
        return NULL;
    }
    // in async case, hasNext may change from assertion above til here
    assert(hasNext());
    assert(buf);
    assert(bpos < blockSize);

    generic_page* ps = (generic_page*) (buf + bpos);
    prevPageNo = ps->pid;

    bpos += PAGE_SIZE;
    bool eof = bpos > bytesRead;
    if (asyncBuf) {
        // in async case, bytesRead does not necessarily relate to the current
        // block in buf, so we must also check the finished flag
        eof = eof && asyncBuf->isFinished();
    }
    if (bpos >= blockSize || eof)
    {
        assert(asyncBuf || bpos == bytesRead);
        bpos = 0;
    }

    count++;
    currentPage = *ps;

    if (asyncBuf && !hasNext()) {
        asyncBuf->consumerRelease();
    }

    return &currentPage;
}

void PageIterator::seek(size_t pageIndex)
{
    assert(!asyncBuf);
    fpos = pageIndex * PAGE_SIZE;
    bpos = 0;
    readBlock(buf);
}

void PageIterator::run()
{
    cout << "Iterator starting" << endl;
    assert(asyncBuf);
    while (!asyncBuf->isFinished()) {
        char* b = asyncBuf->producerRequest();
        readBlock(b);
        if (!in.is_open()) {
            asyncBuf->set_finished();
        }
        asyncBuf->producerRelease();
    }
    cout << "Iterator finished" << endl;
}
