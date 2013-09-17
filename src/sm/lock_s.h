/*
 * (c) Copyright 2011-2013, Hewlett-Packard Development Company, LP
 */

#ifndef LOCK_S_H
#define LOCK_S_H

#include "w_defines.h"

#include "w_key.h"
#include "w_hashing.h"

class lock_base_t : public smlevel_1 {
public:
    // Their order is significant.

    enum status_t {
        t_no_status = 0,
        t_granted = 1,
        t_converting = 2,
        t_waiting = 4
    };

    typedef smlevel_0::lock_mode_t lmode_t;

    enum {
        MIN_MODE = NL, MAX_MODE = XU,
        NUM_MODES = MAX_MODE - MIN_MODE + 1
    };

    static const char* const         mode_str[NUM_MODES];
    static const bool                compat[NUM_MODES][NUM_MODES];
    static const lmode_t             supr[NUM_MODES][NUM_MODES];
};

/**
 * \brief The means of identifying a desired or held lock
 * \details
 * Lock manager requests (acquire, release, query) take an argument
 * of this kind to identify the entity to be locked.
 *
 * Currently, we have only lockid_t made from key.
 * No other lock type exists in the main lock table.
 * For intent locks, we use LIL (light-weight intent lock) which
 * are separated from the main lock table.
 * So, this object is drastically simplified from original Shore-MT.
 */
class lockid_t {
private:
    /**
     * l and w are for convenience
     * w[0] == vol
     * w[1] == store
     * w[2,3]== l[1] == key (its hash)
     */
    union {
        uint64_t l[2]; 
        uint32_t w[4]; 
    };

public:
    /**\brief comparison operator for lockid_t, used by lock manager */
    bool operator<(lockid_t const &p) const;
    /**\brief equality operator for lockid_t*/
    bool operator==(const lockid_t& p) const;
    /**\brief inequality operator for lockid_t*/
    bool operator!=(const lockid_t& p) const;
    friend ostream& operator<<(ostream& o, const lockid_t& i);
public:
    /// Used by lock cache
    uint32_t          hash() const; // used by lock_cache
    /// clear out the lockid - initialize to mean nothing
    void              zero();

    //
    // vid - volume
    //
public:
    /// extract volume id lockid whose lspace() == t_vol or has parent with lspace() == t_vol
    vid_t             vid() const;
private:
    void              set_vid(const vid_t & v);

    //
    // store - stid
    //
public:
    /// extract store number lockid whose lspace() == t_store or has parent with lspace() == t_store
    snum_t            store() const;
private:
    void              set_snum(snum_t s);
    void              set_store(const stid_t & s);


public:

    // construct vacuous lockid_t
    NORET             lockid_t() ;    

    /// construct from key string in an index
    NORET             lockid_t(const stid_t& stid, const unsigned char *keystr, int16_t keylen);
    /// construct from key string in an index
    NORET             lockid_t(const stid_t& stid, const w_keystr_t &key);
    /// copy constructor
    NORET             lockid_t(const lockid_t& i);        

    /// extract a full store id from a store or key lock
    void              extract_stid(stid_t &s) const;

    /// copy operator
    lockid_t&         operator=(const lockid_t& i);
private:
    void _init_for_str(const stid_t &stid, const unsigned char *keystr, int16_t keylen);
};


inline vid_t lockid_t::vid() const 
{
    return (vid_t) w[0];
}

inline void lockid_t::set_vid(const vid_t & v) 
{
    w[0] = v.vol;
}

inline snum_t lockid_t::store() const 
{
    w_assert9(sizeof(snum_t) == sizeof(w[1]));
    return w[1];
}

inline void lockid_t::set_snum(snum_t _s) 
{
    w_assert9(sizeof(snum_t) == sizeof(w[1]));
    w[1] = (uint32_t) _s;
}

inline void lockid_t::set_store(const stid_t & _s) 
{
    set_snum(_s.store);
    set_vid(_s.vol);
}

inline void lockid_t::extract_stid(stid_t &_s) const 
{
    _s.vol = vid();
    _s.store = store();
}

inline void lockid_t::zero()
{
    l[0] = l[1] = 0;
}

inline NORET lockid_t::lockid_t()
{
    zero(); 
}

// use fixed seed for repeatability and easier debugging
const uint32_t LOCKID_T_HASH_SEED = 0xEE5C61DD;


inline void
lockid_t::_init_for_str(const stid_t &stid, const unsigned char *keystr, int16_t keylen)
{
    zero();
    set_store (stid);
    l[1] = w_hashing::uhash::hash64(LOCKID_T_HASH_SEED, keystr, keylen);
}

inline NORET
lockid_t::lockid_t(const stid_t &stid, const unsigned char *keystr, int16_t keylen)
{
    _init_for_str (stid, keystr, keylen);
}
inline NORET
lockid_t::lockid_t(const stid_t &stid, const w_keystr_t &key)
{
    _init_for_str (stid, (const unsigned char*) key.buffer_as_keystr(), key.get_length_as_keystr());
}

inline lockid_t&
lockid_t::operator=(const lockid_t& i)
{
    l[0] = i.l[0];
    l[1] = i.l[1];
    return *this;
}

inline bool
lockid_t::operator<(lockid_t const &i) const
{
    if (l[0] < i.l[0]) return true;
    if (l[0] > i.l[0]) return false;
    return (l[1] < i.l[1]);
}

inline bool
lockid_t::operator==(const lockid_t& i) const
{
    return (l[0] == i.l[0] && l[1] == i.l[1]);
}

inline NORET
lockid_t::lockid_t(const lockid_t& i)
{
    (void) this->operator=(i);
}

inline bool
lockid_t::operator!=(const lockid_t& i) const
{
    return !(l[0] == i.l[0] && l[1] == i.l[1]);
}

const uint32_t LOCKID_T_HASH_MULT = 0x35D0B891;

inline uint32_t lockid_t::hash() const
{
    uint64_t h = w[0];
    h = h * LOCKID_T_HASH_MULT + w[1];
    h = h * LOCKID_T_HASH_MULT + w[2];
    h = h * LOCKID_T_HASH_MULT + w[3];
    return ((uint32_t) (h >> 32)) ^ ((uint32_t) (h & 0xFFFFFFFF));
}

#endif          /*</std-footer>*/
