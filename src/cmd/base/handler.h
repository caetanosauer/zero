#ifndef HANDLER_H
#define HANDLER_H


#include "logrec.h"

#include <iostream>
#include <sstream>
#include <unordered_set>
#include <algorithm>

class Handler {
public:
    //invoke performs handler activity
    virtual void invoke(logrec_t &r) = 0;
    virtual void finalize() = 0;

    virtual void newFile(const char* /* fname */) {};

    virtual ~Handler() {};
};

class PageHandler {
public:
    virtual void finalize() {};
    virtual void handle(const generic_page& page) = 0;
};

class StoreHandler {
public:
    virtual void finalize() {};
    virtual void handle(const StoreID&) = 0;
};

#endif
