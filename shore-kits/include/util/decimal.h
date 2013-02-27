/* -*- mode:C++; c-basic-offset:4 -*-
     Shore-kits -- Benchmark implementations for Shore-MT
   
                       Copyright (c) 2007-2009
      Data Intensive Applications and Systems Labaratory (DIAS)
               Ecole Polytechnique Federale de Lausanne
   
                         All Rights Reserved.
   
   Permission to use, copy, modify and distribute this software and
   its documentation is hereby granted, provided that both the
   copyright notice and this permission notice appear in all copies of
   the software, derivative works or modified versions, and any
   portions thereof, and that both notices appear in supporting
   documentation.
   
   This code is distributed in the hope that it will be useful, but
   WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. THE AUTHORS
   DISCLAIM ANY LIABILITY OF ANY KIND FOR ANY DAMAGES WHATSOEVER
   RESULTING FROM THE USE OF THIS SOFTWARE.
*/

#ifndef __DECIMAL_H
#define __DECIMAL_H

#include <stdint.h>

/** A fixed-point decimal class. Able to represent up to 2^50 cents
    accurately, allowing numbers approaching 4 trillion or so.
 */
class decimal {
    int64_t _value;
    explicit decimal(int64_t value)
	: _value(value)
    {
    }
public:
    decimal()
	: _value(0)
    {
    }
    decimal(double value)
	: _value((int64_t) (value*100))
    {
    }
    decimal(int value)
	: _value((int64_t) (value*100))
    {
    }
    decimal &operator+=(decimal const &other) {
	_value += other._value;
	return *this;
    }
    decimal &operator-=(decimal const &other) {
	_value -= other._value;
	return *this;
    }
    decimal operator*=(decimal const &other) {
	_value = (_value*other._value+50)/100;
	return *this;
    }
    decimal operator/=(decimal const &other) {
	_value = (100*_value+50)/other._value;
	return *this;
    }
    
    decimal operator+(decimal const &other) const {
	return decimal(*this) += other;
    }
    decimal operator-(decimal const &other) const {
	return decimal(*this) -= other;
    }
    decimal operator*(decimal const &other) const {
	return decimal(*this) *= other;
    }
    decimal operator/(decimal const &other) const {
	return decimal(*this) /= other;
    }

    decimal &operator++() {
	_value += 100;
	return *this;
    }
    decimal operator++(int) {
	decimal old = *this;
	++*this;
	return old;
    }
    decimal &operator--() {
	_value -= 100;
	return *this;
    }
    decimal operator--(int) {
	decimal old = *this;
	--*this;
	return old;
    }
    

    double to_double() const {
	return ((double)_value/(double)100);
    }
    long long to_long() const {
	return (_value+50)/100;
	    }
    int to_int() const {
	return (int) to_long();
    }
    
    bool operator <(decimal const &other) const {
	return _value < other._value;
    }
    bool operator >(decimal const &other) const {
	return _value > other._value;
    }
    bool operator ==(decimal const &other) const {
	return _value == other._value;
    }
    bool operator <=(decimal const &other) const {
	return _value <= other._value;
    }
    bool operator >=(decimal const &other) const {
	return _value >= other._value;
    }
    bool operator !=(decimal const &other) const {
	return _value != other._value;
    }
    
};

inline decimal operator+(int a, decimal const &b) {
    return decimal(a) + b;
}
inline decimal operator-(int a, decimal const &b) {
    return decimal(a) - b;
}
inline decimal operator*(int a, decimal const &b) {
    return decimal(a) * b;
}
inline decimal operator/(int a, decimal const &b) {
    return decimal(a) / b;
}

inline decimal operator+(double a, decimal const &b) {
    return decimal(a) + b;
}
inline decimal operator-(double a, decimal const &b) {
    return decimal(a) - b;
}
inline decimal operator*(double a, decimal const &b) {
    return decimal(a) * b;
}
inline decimal operator/(double a, decimal const &b) {
    return decimal(a) / b;
}


#endif
