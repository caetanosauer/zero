#ifndef ITERATOR_H
#define ITERATOR_H


#include "sm_base.h"
#include "generic_page.h"

#include<fstream>

#include "ringbuffer.h"

class PageIterator : public smthread_t
{
public:
    static const size_t PAGE_SIZE;
    PageIterator(string inPath, string outPath,
            unsigned ioSizeInPages = 128);
    PageIterator(string inPath, string outPath, AsyncRingBuffer* buffer,
            unsigned ioSizeInPages = 128);
    virtual ~PageIterator();

    generic_page* next();
    bool hasNext();
    long getCount() { return count; }
    void writePage(char* buf, size_t index);
    virtual void run();

    // for debug
    void seek(size_t pageIndex);
private:
    string inPath;
    string outPath;
    size_t blockSize;
    ifstream in;
    ofstream out;
    long count;
    off_t fpos;
    size_t bpos;
    size_t bytesRead;
    int blocksRead;
    char* buf;
    generic_page currentPage;
    PageID prevPageNo;
    AsyncRingBuffer* asyncBuf;

    void openOutput();
    void readBlock(char* b);
    void writeBlock(char* b);
};

#endif
