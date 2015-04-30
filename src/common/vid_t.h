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
typedef uint16_t vid_t;

/** maximum number of volumes that can exist. 1 to 65536. */
#define MAX_VOL_COUNT 256

#endif
