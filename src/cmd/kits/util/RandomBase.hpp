#ifndef LINTEL_RANDOM_BASE_HPP
#define LINTEL_RANDOM_BASE_HPP

#include <stdint.h>
#include <vector>

#ifndef UINT32_MAX 
#define UINT32_MAX (0xFFFFFFF)
#endif

namespace lintel {

    template <class R>
    class RandomTempl : public R{
    public:

        RandomTempl(uint32_t seed = 0) : R(seed), boolShift(0), boolState(0) {            
        }

        RandomTempl(std::vector<uint32_t> seed_array) : R(seed_array), boolShift(0), boolState(0) {
        }

        ~RandomTempl() { }

        inline void init(uint32_t seed) {
            R::init(seed);
        }

        inline void initArray(std::vector<uint32_t> seed_array) {
            R::initArray(seed_array);
        }       

        inline uint32_t randInt() {
            return R::randInt();
        }

        inline unsigned long long randLongLong() {
            unsigned long long ret;
            ret = randInt();
            ret = ret << 32;
            ret = ret | randInt();
            return ret;
        }
        

        // Slightly biased
        inline uint32_t randInt(uint32_t max) {
            return randInt() % max;
        }

        inline uint32_t randIntUnbiased(uint32_t max) {
            do {
                uint32_t res = randInt();
                if (UINT32_MAX-max > res) { 
                    //Quick test to handle most cases
                    return res % max;
                } else if ((UINT32_MAX / max) * max > res) { 
                    //Slow test to handle the biased corner
                    return res % max;
                }
            } while (1);            
        }

        // randDouble() gives you doubles with 32 bits of randomness.
        // randLongDouble() gives you doubles with ~53 bits of randomness
        // *Open() gives values in [0,1)
        // *Closed() gives values in [0,1]
        
        // HP-UX aCC won't let me define these as const double;
        // const double foo = 5.0 reports error 481.
#define MTR_int_to_open (1.0/4294967296.0) 
#define MTR_int_to_closed (1.0/4294967295.0) 
#define MTR_AMult (67108864.0) // 2^(32-6)
        // 9007199254740992 = 2^53, 9007199254740991 = 2^53 -1
#define MTR_53bits_to_open(a,b) ((a * MTR_AMult + b)/9007199254740992.0)
#define MTR_53bits_to_closed(a,b) ((a * MTR_AMult + b)/9007199254740991.0)
        inline double randDoubleOpen() { // in [0,1), 32 bits of randomness
            return (double)randInt() * MTR_int_to_open;
        }
        inline double randDoubleClosed() {  // in [0,1], 32 bits of randomness
            return (double)randInt() * MTR_int_to_closed;
        }
        inline double randDouble() { // in [0,1), 32 bits of randomness
            return randDoubleOpen();
        }
        
        // casting a long long to double is unbelievably slow on pa2.0 aCC C.03.30
        inline double randDoubleOpen53() { // in [0,1), 53 bits of randomness
            uint32_t a=randInt()>>5, b=randInt()>>6;
            return MTR_53bits_to_open(a,b);
        }
        inline double randDoubleClosed53() { // in [0,1]
            uint32_t a=randInt()>>5, b=randInt()>>6;
            return MTR_53bits_to_closed(a,b);
        }
        
        inline bool randBool() {
            return (randInt() & 0x1) ? true : false;
        }        

        inline bool randBoolFast() {
            if (boolShift==0) {
                boolState = randLongLong();
                boolShift = 64;
            }
            bool ret = boolState & 0x1;
            boolState >>= 1;
            --boolShift;
            return ret;
        }
    protected:
        uint8_t boolShift;
        uint64_t boolState;
    };
};

#endif
