/*
 * hash_32 - 32 bit Fowler/Noll/Vo FNV-1a hash code
 *
 * @(#) $Revision: 1.1 $
 * @(#) $Id: hash_32a.c,v 1.1 2003/10/03 20:38:53 chongo Exp $
 * @(#) $Source: /usr/local/src/cmd/fnv/RCS/hash_32a.c,v $
 *
 ***
 *
 * Fowler/Noll/Vo hash
 *
 * The basis of this hash algorithm was taken from an idea sent
 * as reviewer comments to the IEEE POSIX P1003.2 committee by:
 *
 *      Phong Vo (http://www.research.att.com/info/kpv/)
 *      Glenn Fowler (http://www.research.att.com/~gsf/)
 *
 * In a subsequent ballot round:
 *
 *      Landon Curt Noll (http://www.isthe.com/chongo/)
 *
 * improved on their algorithm.  Some people tried this hash
 * and found that it worked rather well.  In an EMail message
 * to Landon, they named it the ``Fowler/Noll/Vo'' or FNV hash.
 *
 * FNV hashes are designed to be fast while maintaining a low
 * collision rate. The FNV speed allows one to quickly hash lots
 * of data while maintaining a reasonable collision rate.  See:
 *
 *      http://www.isthe.com/chongo/tech/comp/fnv/index.html
 *
 * for more details as well as other forms of the FNV hash.
 ***
 *
 * To use the recommended 32 bit FNV-1a hash, pass FNV1_32A_INIT as the
 * uint32_t hashval argument to fnv_32a_buf() or fnv_32a_str().
 *
 ***
 *
 * Please do not copyright this code.  This code is in the public domain.
 *
 * LANDON CURT NOLL DISCLAIMS ALL WARRANTIES WITH REGARD TO THIS SOFTWARE,
 * INCLUDING ALL IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS. IN NO
 * EVENT SHALL LANDON CURT NOLL BE LIABLE FOR ANY SPECIAL, INDIRECT OR
 * CONSEQUENTIAL DAMAGES OR ANY DAMAGES WHATSOEVER RESULTING FROM LOSS OF
 * USE, DATA OR PROFITS, WHETHER IN AN ACTION OF CONTRACT, NEGLIGENCE OR
 * OTHER TORTIOUS ACTION, ARISING OUT OF OR IN CONNECTION WITH THE USE OR
 * PERFORMANCE OF THIS SOFTWARE.
 *
 * By:
 *	chongo <Landon Curt Noll> /\oo/\
 *      http://www.isthe.com/chongo/
 *
 * Share and Enjoy!	:-)
 */

#include "util/fnv.h"


/*
 * 32 bit magic FNV-1a prime
 */
#define FNV_32_PRIME ((uint32_t)0x01000193)


/*
 * fnv_32a_buf - perform a 32 bit Fowler/Noll/Vo FNV-1a hash on a buffer
 *
 * input:
 *	buf	- start of buffer to hash
 *	len	- length of buffer in octets
 *	hval	- previous hash value or 0 if first call
 *
 * returns:
 *	32 bit hash as a static hash type
 *
 * NOTE: To use the recommended 32 bit FNV-1a hash, use FNV1_32A_INIT as the
 * 	 hval arg on the first call to either fnv_32a_buf() or fnv_32a_str().
 */
#if 0
uint32_t
fnv_hash(const char *bp, size_t len, uint32_t hval)
{
    const char *be = bp + len;		/* beyond end of buffer */

    /*
     * FNV-1a hash each octet in the buffer
     */
    while (bp < be) {

	/* xor the bottom with the current octet */
	hval ^= (uint32_t)*bp++;

	/* multiply by the 32 bit FNV magic prime mod 2^32 */
#if defined(NO_FNV_GCC_OPTIMIZATION)
	hval *= FNV_32_PRIME;
#else
	hval += (hval<<1) + (hval<<4) + (hval<<7) + (hval<<8) + (hval<<24);
#endif
    }

    /* return our new hash value */
    return hval;
}

#else
/*
 * This version should produce identical output, but uses Duff's
 * Device to unroll the loop 8x. It should be measurably faster than
 * the original version. (I got 5.5x speedup over gcc and 3x speedup
 * over g++, both with -O3 and -fomit-frame-pointer and full
 * inlining. Without inlining, they execute at the same speed for
 * short (4-byte) keys, but longer keys should still be better)
 */
static inline
uint32_t mix(char val, uint32_t hval) {
    hval ^= (uint32_t) val;
    hval += (hval<<1) + (hval<<4) + (hval<<7) + (hval<<8) + (hval<<24);
    return hval;
}

uint32_t
fnv_hash(const char *bp, size_t len, uint32_t hval)
{
    // how many (partial) loops are there?
    const char *be = bp + len - 8;
    
    // correct the "starting" address before jumping. If len is a
    // multiple of 8 this is results in an incorrect address, but the problem
    // is fixed in the loop body.
    bp -= 8 - (len % 8);

    // jump directly to the case that handles the partial loop
    // properly
    switch(len % 8) {
    case 0:
        do { 	/* xor the bottom with the current octet */
            // increment the pointer (first time through, with modulus
            // == 0, this corrects the -= 8 that was done before. The
            // rest of the time it prepares for the next loop body.
            bp += 8;
            hval = mix(bp[0], hval);
        case 7:
            hval = mix(bp[1], hval);
        case 6:            
            hval = mix(bp[2], hval);
        case 5:
            hval = mix(bp[3], hval);
        case 4:
            hval = mix(bp[4], hval);
        case 3:
            hval = mix(bp[5], hval);
        case 2:
            hval = mix(bp[6], hval);
        case 1:
            hval = mix(bp[7], hval);
        } while (bp < be);
    }

    /* return our new hash value */
    return hval;
}
#endif // #ifdef USE_ORIGINAL

#if 0
// Test driver
int main() {
    uint32_t hash = FNV_INIT;
    unsigned i[2];
    for(*i=0; *i < (1 << 28); ++*i)
        hash ^= fnv_hash((char*) &i, sizeof(i), hash);

    return 0;
}
#endif
