
#ifndef __HASH_FUNCTION_H
#define __HASH_FUNCTION_H


/* exported functions */

unsigned int RSHash  (const char* str, unsigned int len);
unsigned int JSHash  (const char* str, unsigned int len);
unsigned int PJWHash (const char* str, unsigned int len);
unsigned int ELFHash (const char* str, unsigned int len);
unsigned int BKDRHash(const char* str, unsigned int len);
unsigned int SDBMHash(const char* str, unsigned int len);
unsigned int DJBHash (const char* str, unsigned int len);
unsigned int DEKHash (const char* str, unsigned int len);
unsigned int APHash  (const char* str, unsigned int len);


#endif
