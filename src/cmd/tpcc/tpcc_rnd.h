/*
 * (c) Copyright 2013-2014, Hewlett-Packard Development Company, LP
 */

#ifndef EXPERIMENTS_TPCC_RND_H
#define EXPERIMENTS_TPCC_RND_H

#include <stdint.h>

namespace tpcc {
    /** thread-local-random. it's also deterministic. */
    class tlr_t {
    public:
        tlr_t (uint64_t seed) : _seed(seed) {}

        /**
         * In TPCC terminology, from=x, to=y.
         * NOTE both from and to are _inclusive_.
         */
        uint32_t uniform_within(uint32_t from, uint32_t to) {
            return from + (next_uint32() % (to - from + 1));
        }
        /**
         * Same as uniform_within() except it avoids the "except" value.
         * Make sure from!=to.
         */
        uint32_t uniform_within_except(uint32_t from, uint32_t to, uint32_t except) {
            while (true) {
                uint32_t val = uniform_within(from, to);
                if (val != except) {
                    return val;
                }
            }
        }
        
        /**
         * Non-Uniform random (NURand) in TPCC spec (see Sec 2.1.6).
         * In TPCC terminology, from=x, to=y.
         *  NURand(A, x, y) = (((random(0, A) | random(x, y)) + C) % (y - x + 1)) + x
         */
        uint32_t non_uniform_within(uint32_t A, uint32_t from, uint32_t to) {
            uint32_t C = get_c(A);
            return  (((uniform_within(0, A) | uniform_within(from, to)) + C) % (to - from + 1)) + from;
        }
        
        uint64_t get_current_seed() {
            return _seed;
        }
        void set_current_seed(uint64_t seed) {
            _seed = seed;
        }
    private:
        uint32_t next_uint32() {
            _seed = _seed * 0xD04C3175 + 0x53DA9022;
            return (_seed >> 32) ^ (_seed & 0xFFFFFFFF);
        }
        uint64_t _seed;
        
        /**
         * C is a run-time constant randomly chosen within [0 .. A] that can be
         * varied without altering performance. The same C value, per field
         * (C_LAST, C_ID, and OL_I_ID), must be used by all emulated terminals.
         * constexpr, but let's not bother C++11.
         */
        uint32_t get_c(uint32_t A) const {
            // yes, I'm lazy. but this satisfies the spec.
            const uint64_t C_SEED = 0x734b00c6d7d3bbdaULL;
            return C_SEED % (A + 1);
        }
    };
}
#endif // EXPERIMENTS_TPCC_RND_H
