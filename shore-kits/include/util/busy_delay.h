
/** @file busy_delay.h
 *
 *  @brief Exports busy_delay operations.
 *
 *  @author Naju Mancheril (ngm)
 *
 *  @bug See busy_delay.c.
 */
#ifndef _BUSY_DELAY_H
#define _BUSY_DELAY_H


/* exported functions */

int  busy_delay_init(void);
void busy_delay_ms(int ms);
void busy_delay_us(int us);


#endif
