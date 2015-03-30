#ifndef VID_T_H
#define VID_T_H

#include "w_defines.h"

#include <iostream>

/**\brief Volume ID. See \ref IDS.
 *\ingroup IDS
 * \details
 * This class represents a volume identifier, the id that is persistent
 * in the database. Its size is two bytes.
 *
 * A volume id is part of record identifiers and store identifiers,
 * as well as part of "long" page identifiers.
 *
 * See \ref IDS.
 */
typedef uint16_t volid_t;

/** maximum number of volumes that can exist. 1 to 65536. */
#define MAX_VOL_COUNT 256

/**\brief Volume ID. See \ref IDS.
 *\ingroup IDS
 * \details
 * This class represents a volume identifier, the id that is persistent
 * in the database. It is usually a short integer. 
 * Its size is two bytes.
 *
 * A volume id is part of record identifiers and store identifiers,
 * as well as part of "long" page identifiers.
 *
 * See \ref IDS.
 */
struct vid_t {

    enum {
          first_local = 1
         };

                    vid_t() : vol(0) {}
                    vid_t(volid_t v) : vol(v) {}
    void        init_local()        {vol = first_local;}

    void        incr_local()        {
                                    vol++;
                                }

    // This function casts a vid_t to a uint16_t.  It is needed
    // in lid_t.h where there is a hack to use vid_t to
    // create a long volume ID.
                    operator uint16_t () const {return vol;}

    // Data Members
    volid_t        vol;

    static const vid_t null;
    friend inline std::ostream& operator<<(std::ostream&, const vid_t& v);
    friend inline std::istream& operator>>(std::istream&, vid_t& v);
    friend bool operator==(const vid_t& v1, const vid_t& v2)  {
        return v1.vol == v2.vol;
    }
    friend bool operator!=(const vid_t& v1, const vid_t& v2)  {
        return v1.vol != v2.vol;
    }
    friend bool operator>(const vid_t& v1, const vid_t& v2)  {
        return v1.vol > v2.vol;
    }
};

inline std::ostream& operator<<(std::ostream& o, const vid_t& v)
{
    return o << v.vol;
}
 
inline std::istream& operator>>(std::istream& i, vid_t& v)
{
    return i >> v.vol;
}

#endif
