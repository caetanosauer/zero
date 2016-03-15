#ifndef W_FILL_H
#define W_FILL_H

#include <stdint.h>

/*
 * These types are auto-initialized filler space for alignment
 * in structures.  The auto init helps with purify/valgrind.
 */
/** \brief Auto-initialized 1-byte (8-bits) filler for alignment of structures*/
struct fill8 {
    uint8_t u;
    fill8() : u(0) {}
};

/** \brief Auto-initialized 2-byte (16-bits) filler for alignment of structures*/
struct fill16 {
    uint16_t u;
    fill16() : u(0) {}
};

/** \brief Auto-initialized 3-byte (24-bits)  filler for alignment of structures*/
struct fill24 {
    uint8_t    u[3];
    fill24() { u[0] = u[1] = u[2] = 0; }
};

/** \brief Auto-initialized 4-byte (32-bits)  filler for alignment of structures*/
struct fill32 {
    uint32_t u;
    fill32() : u(0) {}
};

// followings are for older codes that use "number of bytes" naming.
// when all of such codes are kicked out from our code base, these should go away too.
struct fill1 {
    uint8_t u;
    fill1() : u(0) {}
};
struct fill2 {
    uint16_t u;
    fill2() : u(0) {}
};
struct fill3 {
    uint8_t    u[3];
    fill3() { u[0] = u[1] = u[2] = 0; }
};
struct fill4 {
    uint32_t u;
    fill4() : u(0) {}
};

#endif // W_FILL_H
