#include "tls.h"
#include "util/random_input.h"

// Random number generator stored in TLS
DECLARE_TLS(randgen_t, randgen_tls);

const char CAPS_CHAR_ARRAY[]  = { "ABCDEFGHIJKLMNOPQRSTUVWXYZ" };
const char NUMBERS_CHAR_ARRAY[] = { "012345789" };

int URand(const int low, const int high)
{
  randgen_t* randgenp = randgen_tls.get();
  assert (randgenp);

  int d = high - low + 1;
  return (low + randgenp->rand(d));
}



bool
URandBool()
{
    return (URand(0,1) ? true : false);
}


short
URandShort(const short low, const short high)
{
  thread_t* self = thread_get_self();
  assert (self);
  randgen_t* randgenp = self->randgen();
  assert (randgenp);

  short d = high - low + 1;
  return (low + (short)randgenp->rand(d));
}


void
URandFillStrCaps(char* dest, const int sz)
{
    assert (dest);
    for (int i=0; i<sz; i++) {
        dest[i] = CAPS_CHAR_ARRAY[URand(0,sizeof(CAPS_CHAR_ARRAY)-1)];
    }
}


void
URandFillStrNumbx(char* dest, const int sz)
{
    assert (dest);
    for (int i=0; i<sz; i++) {
        dest[i] = NUMBERS_CHAR_ARRAY[URand(0,sizeof(NUMBERS_CHAR_ARRAY)-1)];
    }
}


#define USE_ZIPF 1

bool _g_enableZipf = false;
double _g_ZipfS = 0.0;

//Zipfian between low and high
int ZRand(const int low, const int high)
{
	zipfian myZipf(high-low+2,_g_ZipfS);

	thread_t* self = thread_get_self();
	assert (self);
	randgen_t* randgenp = self->randgen();
	assert (randgenp);
	double u = (double)randgenp->rand(10000)/double(10000);

	return (myZipf.next(u)+low-1);
}

void setZipf(const bool isEnabled, const double s)
{
    _g_enableZipf = isEnabled;
    _g_ZipfS = s;
}

//If enableZip is set to 1 then return zipfian else returns uniform
int UZRand(const int low, const int high)
{
#ifdef USE_ZIPF
	return ( _g_enableZipf? ( ZRand(low,high) ):( URand(low,high) ));
#else
        return URand(low,high);
#endif
}



