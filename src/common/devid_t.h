#ifndef DEVID_T_H
#define DEVID_T_H

#include "w_defines.h"

#include "w_fill.h"
#include <iosfwd>

/**\brief Internal Device ID
 *
 * \ingroup IDS
 *
 * \details
 * This identifier is not persistent; it is assigned when
 * a device is mounted (by the filesystem's file name (a string))
 */
struct devid_t {
    uint64_t    id;
    uint32_t    dev;
#ifdef ZERO_INIT
    fill32    dummy;
#endif

    devid_t() : id(0), dev(0) {};
    devid_t(const char* pathname);

    bool operator==(const devid_t& d) const {
        return id == d.id && dev == d.dev;
    }

    bool operator!=(const devid_t& d) const {return !(*this==d);}
    friend std::ostream& operator<<(std::ostream&, const devid_t& d);

    static const devid_t null;
};

#endif 
