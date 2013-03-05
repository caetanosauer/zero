#ifndef SUPPRESS_UNUSED_H
#define SUPPRESS_UNUSED_H
#include <assert.h>

//Macros to help suppress warnings in unused functions.

//Default case is to suppress
#ifdef SHOW_ALL_UNUSED_VARS
#define SUPPRESS_UNUSED(x) do{}while (0)
#else //TODO could have special mode to warn all at once?  Or special warn things in the macro?
#define SUPPRESS_UNUSED(x) do{(void) (x);}while (0)
#endif


#define SUPPRESS_UNUSED_2(x, y) do{SUPPRESS_UNUSED(x); SUPPRESS_UNUSED(y);}while(0)
#define SUPPRESS_UNUSED_3(x, y, z) do{SUPPRESS_UNUSED(x); SUPPRESS_UNUSED_2(y, z);}while(0)
#define SUPPRESS_UNUSED_4(x, y, z, a) do{SUPPRESS_UNUSED_2(x, y); SUPPRESS_UNUSED_2(z, a);}while(0)
#define SUPPRESS_UNUSED_5(a,b,c,d,e) do{SUPPRESS_UNUSED_2(a,b); SUPPRESS_UNUSED_3(c,d,e);}while(0)
#define SUPPRESS_UNUSED_6(a,b,c,d,e,f) do{SUPPRESS_UNUSED_3(a,b,c);SUPPRESS_UNUSED_3(d,e,f);}while(0)

#define SUPPRESS_NON_RETURN(type) do{assert(0); static type throwaway; return throwaway;}while(1);
#endif
