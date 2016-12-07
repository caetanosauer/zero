#ifndef W_RC_H
#define W_RC_H
/*
 * (c) Copyright 2014, Hewlett-Packard Development Company, LP
 */

/**
 * \defgroup RCT Return values
 * \brief Standard return values of functions in storage manager
 * \ingroup IDIOMS
 * \details
 * The storage manager is written with programming idioms to make sure
 * all return codes are checked, and as a user of the storage manager, you
 * strongly encouraged to use these idioms.
 *
 * It is especially important that you understand the storage manager's
 * \e return \e code type, \ref w_rc_t.
 * Most of the storage manager methods return this type.  The return code
 * type is a dynamically-allocated class instance (except when RCOK, the
 * default, non-error code, is returned: this is a static constant).
 *
 * Several macros defined in w_rc.h support writing and using methods
 * and functions that return a w_rc_t.
 * \code
 w_rc_t func(...) {
     W_DO(ss_m::create_assoc(...));
     return RCOK;
 }
 \endcode
 *
 * The \ref W_DO macro returns whatever the called function returns if
 * that return code was an error code, otherwise, it falls through
 * to the code below the macro call. This is the most-often used
 * idiom.
 *
 * The RC_* macros  let you construct a return code for a return value
 * from a function.  The normal, non-error return code is \ref RCOK.
 *
 * Individual error codes and corresponding error messages are described in detail by \ref ERRORCODES.
 * There you wil also see how to create your own return codes.
 *
 * \section Difference Difference from Shore-MT
 * Like many other modules in the storage manager,
 * these classes went through a complete refactoring when we convert Shore-MT to Foster-Btrees.
 * Virtually the only thing that was unchanged is the name (w_rc_t, RCOK, W_DO, etc).
 * We do not use TLS for w_rc_t. We do not have a perl script to generate error code enum. etc.
 * This file is completely independent and header-only. Just include w_rc.h to use.
 */
#include <stdint.h>
#include <memory.h> // for memcpy
#include <ostream> // for pretty-printing w_rc_t
#include <iostream> // for cout
#include <sstream> // for append
#include <cassert>
#include <cstdlib> // for abort
#include "w_error.h"

/**
 * \brief Constant to define maximum stack trace depth for w_rc_t.
 * \ingroup RCT
 */
const uint16_t MAX_RCT_STACK_DEPTH = 8;

/**
 * \brief Return code for most functions and methods.
 * \ingroup RCT
 * \details
 * \section MaxDepth Maximum stack trace depth
 * When the return code is an error code, we propagate back the stack trace
 * for easier debugging. Original Shore-MT had a linked-list for this
 * and, to ameriolate allocate/delete cost for it, had a TLS object pool.
 * Unfortunately, it caused issues in some environments and was not readable/maintainable.
 * Instead, Foster-BTree limits the depth of stacktraces stored in w_rc_t to a reasonable number
 * enough for debugging; \ref MAX_RCT_STACK_DEPTH.
 * We then store just line numbers and const pointers to file names. No heap allocation.
 * The only thing that has to be allocated on heap is a custom error message.
 * However, there are not many places that use custom error messages, so the cost usually doesn't happen.
 *
 * \section ForcedCheck Forced return code checking
 * An error code must be checked by some code, else it will report an
 *  "error-not-checked" warning in stderr (NOT fatal errors).
 *  This is costly but useful for checking that code copes with
 *  errors.   Of course, it does not do a static analysis; rather it
 *  is a dynamic check and so it cannot catch all code
 *  that ignores return codes.
 */
class w_rc_t {
public:
    /** Empty constructor. This is same as duplicating RCOK. */
    w_rc_t();

    /**
     * \brief Instantiate a return code without a custom error message nor stacktrace.
     * @param[in] error_code Error code, either OK or real errors.
     * \detail
     * This is the most (next to RCOK) light-weight way to create/propagate a return code.
     * Use this one if you do not need a detail information to debug the error (eg, error whose cause
     * is obvious, an expected error that is immediately caught, etc).
     */
    explicit w_rc_t(w_error_codes error_code);

    /**
     * \brief Instantiate a return code with stacktrace and optionally a custom error message.
     * @param[in] filename file name of the current place.
     *   It must be a const and permanent string, such as what "__FILE__" returns. Note that we do NOT
     * do deep-copy of the strings.
     * @param[in] linenum line number of the current place. Usually "__LINE__".
     * @param[in] error_code Error code, must be real errors.
     * @param[in] custom_message Optional custom error message in addition to the default one inferred from error code.
     * If you pass a non-NULL string to this argument, we do deep-copy for each hand-over, so it's EXPENSIVE!
     */
    w_rc_t(const char* filename, uint32_t linenum, w_error_codes error_code, const char* custom_message = NULL);

    /** Copy constructor. */
    w_rc_t(const w_rc_t &other);

    /** Copy constructor to augment the stacktrace. */
    w_rc_t(const w_rc_t &other, const char* filename, uint32_t linenum, const char* more_custom_message = NULL);

    /** Copy constructor. */
    w_rc_t& operator=(w_rc_t const &other);

    /** Will warn in stderr if the error code is not checked yet. */
    ~w_rc_t();

    /**
     * \brief True if this return code is not RCOK or equivalent.
     * This \b must be called for every w_rc_t before destruction.
     * The idiomatic macros W_DO and its companions do that check for you, and
     * should be used most of the time.
     *
     * See also the following macros:
     * - #W_DO(x)
     * - #W_DO_MSG(x,m)
     */
    bool                is_error() const;

    /** Return the integer error code. */
    w_error_codes       err_num() const;

    /** Returns the error message inferred by the error code. */
    const char*         get_message() const;

    /** Returns the custom error message. */
    const char*         get_custom_message() const;

    /** Appends more custom error message at the end. */
    void                append_custom_message(const char* more_custom_message);

    /** Returns the depth of stack this error code has collected. */
    uint16_t            get_stack_depth() const;

    /** Returns the line number of the given stack position. */
    uint16_t            get_linenum(uint16_t stack_index) const;

    /** Returns the file name of the given stack position. */
    const char*         get_filename(uint16_t stack_index) const;

    /** Output a warning to stderr if the error is not checked yet. */
    void                verify() const;

    /**
     * \brief Fail catastrophically after describing the error.
     * This is called from W_COERCE to handle an unexpected error.
     */
    void                fatal() const;

private:

    /**
     * \brief Filenames of stacktraces.
     * This is deep-first, so _filenames[0] is where the w_rc_t was initially instantiated.
     * When we reach MAX_RCT_STACK_DEPTH, we don't store any more stacktraces and
     * just say ".. more" in the output.
     * We do NOT deep-copy the strings, assuming the file name string is const and
     * permanent. We only copy the pointers when passing around.
     * As far as we use "__FILE__" macro to get file name, this is the always case.
     */
    const char*     _filenames[MAX_RCT_STACK_DEPTH];

    /** \brief Line numbers of stacktraces. */
    uint16_t        _linenums[MAX_RCT_STACK_DEPTH];

    /**
     * \brief Optional custom error message.
     * We deep-copy this string if it's non-NULL.
     * The reason why we don't use auto_ptr etc for this is that they are also expensive and will screw things
     * up if someone misuse our class. Custom error message should be rare, anyways.
     */
    const char*     _custom_message;

    /**
     * \brief Integer error code.
     * \section Invariant Important invariants
     * If this value is w_error_ok, all other members have no meanings and we might not even bother clearing them
     * for better performance because that's by far the common case. So, all functions in this class
     * should first check if this value is w_error_ok or not to avoid further processing.
     */
    w_error_codes   _error_code;

    /**
     * \brief Current stack depth.
     * Value 0 implies that we don't pass around stacktrace for this return code, bypassing stacktrace collection.
     */
    uint16_t        _stack_depth;

    /** \brief Whether someone already checked the error code of this object.*/
    mutable bool    _checked;
};

typedef w_rc_t rc_t;

inline std::ostream& operator<<(std::ostream& o, const w_rc_t& obj) {
    if (obj.err_num() == w_error_ok) {
        o << "No error";
    } else {
        o << w_error_name(obj.err_num()) << "(" << obj.err_num() << "):" << obj.get_message();
        if (obj.get_custom_message() != NULL) {
            o << ":" << obj.get_custom_message();
        }
        for (uint16_t stack_index = 0; stack_index < obj.get_stack_depth(); ++stack_index) {
            o << std::endl << "  " << obj.get_filename(stack_index) << ":" << obj.get_linenum(stack_index);
        }
        if (obj.get_stack_depth() >= MAX_RCT_STACK_DEPTH) {
            o << std::endl << "  .. and more. Increase MAX_RCT_STACK_DEPTH to see full stacktraces";
        }
    }
    return o;
}

/**
 * \def  RCOK
 * \ingroup RCT
 * \brief Normal return value for no-error case.
 * \details
 * Const return code that indicates no error.
 * This is the normal way to return from a method or function.
 */
const w_rc_t RCOK;

/**
 * \def  RC(e)
 * \ingroup RCT
 * \brief Normal error-case return.
 * Create a return code with the current file, line, and error code x.
 * This is the normal way to return from a method or function.
 */
#define RC(e)       w_rc_t(__FILE__, __LINE__, e)

/**
 * \def  RC_AUGMENT(rc)
 * \ingroup RCT
 * \brief Augment stack trace.
 * Add stack trace information (file, line) to a return code.
 * This is the normal way to return from a method or function upon
 * receiving an error from a method or function that it called.
 * Used by \ref #W_DO(x), \ref #W_DO_MSG(x,m),
 * and \ref #W_COERCE(x)
 *  e.g.
 *    w_rc_t rc = create_file(f);
 *      if (rc.is_error())  return RC_AUGMENT(rc);
 *    rc = close_file(f);
 */
#define RC_AUGMENT(rc) w_rc_t(rc, __FILE__, __LINE__)

/**
 * \def  RC_APPEND_MSG(rc, m)
 * \ingroup RCT
 * \brief Appends more arbitrary string information to the return code.
 *
 * Add a char string representing more information to a return code.
 * Used by \ref W_RETURN_RC_MSG(e, m),
 * \ref W_COERCE_MSG(x, m)
 */
#define RC_APPEND_MSG(rc, m)    \
do {                            \
    std::stringstream os;       \
    os m;                       \
    rc.append_custom_message(os.str().c_str()); \
} while (0)

/**
 * \def  W_RETURN_RC_MSG(e, m)
 * \ingroup RCT
 * \brief Retrun with a return code that contains the given error code and additional message.
 */
#define W_RETURN_RC_MSG(e, m)           \
do {                                    \
    w_rc_t __e(__FILE__, __LINE__, e);  \
    RC_APPEND_MSG(__e, m);              \
    return __e;                         \
} while (0)

/**
 * \def  W_DO(x)
 * \ingroup RCT
 * \brief Call a method or function \b x.
 * This macro is the normal idiom for calling a method or function.
 * Most methods and functions return a w_rc_t. This macro calls \b x
 * and checks its returned value.  If an error is encountered, it
 * immediately returns from the current function or method, augmenting
 * the stack trace held by the return code.
 */
#define W_DO(x)         \
do {                    \
    w_rc_t __e(x);      \
    if (__e.is_error()) {return RC_AUGMENT(__e);} \
} while (0)

/**
 * \def  W_DO_MSG(x, m)
 * \ingroup RCT
 * \brief Call a method or function \b x.
 *
 * Like \ref #W_DO, but any error returned contains
 * the additional information message \b m.
 */
#define W_DO_MSG(x, m)          \
do {                            \
    w_rc_t __e = (x);           \
    if (__e.is_error()) {       \
        RC_AUGMENT(__e);        \
        RC_APPEND_MSG(__e, m);  \
        return __e;             \
    }                           \
} while (0)

/**
 * \def  W_COERCE(x)
 * \ingroup RCT
 * \brief Call a function or method \b x, fail catastrophically if error is returned.
 *
 * This macro is like \ref #W_DO(x), but instead of returning in the error
 * case, it fails catastrophically.
 * It is used in cases such as these:
 * - Temporary place-holder where the coder hasn't written
 *   the failure-handling code
 * - The calling function or method API has no means to return
 *   error information, and this case hasn't yet been accommodated.
 * - The called \b x should never return an error
 *   in this case, and doing so would indicate a programming error.
 * - The called \b x never returns an error at the time the calling code
 *   is written, and should the called code \b x change,
 *   the calling code should probably be adjusted to handle any new error.
 *
 *   The call to __e.fatal() prints the stack trace and additional information
 *   associated with the w_rc_t before it croaks.
 */
#define W_COERCE(x)             \
do {                            \
    w_rc_t __e = (x);           \
    if (__e.is_error())  {      \
        __e = RC_AUGMENT(__e);  \
        __e.fatal();            \
    }                           \
} while (0)

/**
 * \def  W_COERCE_MSG(x, m)
 * \ingroup RCT
 * \brief Same as \ref #W_COERCE(x) but adds a string message before croaking.
 */
#define W_COERCE_MSG(x, m)       \
do {                             \
    w_rc_t __em = (x);           \
    if (__em.is_error())  {      \
        __em = RC_AUGMENT(__em); \
        RC_APPEND_MSG(__em, m);  \
        __em.fatal();            \
    }                            \
} while (0)

/**
 * \def  W_FATAL(e)
 * \ingroup RCT
 * \brief Croak with the error code \b e.
 */
#define W_FATAL(e)           W_COERCE(RC(e))

/**
 * \def  W_FATAL_MSG(e, m)
 * \ingroup RCT
 * \brief Croak with the error code \b e and message \b m.
 */
#define W_FATAL_MSG(e, m)    W_COERCE_MSG(RC(e), m)

/**
 * \def  W_IGNORE(x)
 * \ingroup RCT
 * \brief Invoke \b x and ignore its result.
 */
#define W_IGNORE(x)    ((void) x.is_error())

inline w_rc_t::w_rc_t()
    : _custom_message(NULL), _error_code(w_error_ok), _stack_depth(0), _checked(true) {
}

inline w_rc_t::w_rc_t(w_error_codes error_code)
    : _custom_message(NULL), _error_code(error_code), _stack_depth(0), _checked(false) {
}

inline w_rc_t::w_rc_t(const char* filename, uint32_t linenum, w_error_codes error_code, const char* custom_message)
    : _custom_message(custom_message), _error_code(error_code), _stack_depth(1), _checked(false) {
    assert(error_code != w_error_ok);
    _filenames[0] = filename;
    _linenums[0] = linenum;
#if W_DEBUG_LEVEL>=5
    std::cout << "Error instantiated: " << *this << std::endl;
#endif //W_DEBUG_LEVEL>=3
}

inline w_rc_t::w_rc_t(const w_rc_t &other) {
    operator=(other);
}

inline w_rc_t& w_rc_t::operator=(w_rc_t const &other) {
    // Invariant: if w_error_ok, no more processing
    if (other._error_code == w_error_ok) {
        this->_error_code = w_error_ok;
        return *this;
    }

    // As we don't have any linked-list etc, mostly this is enough. Quick.
    ::memcpy(this, &other, sizeof(w_rc_t)); // note, we take copy BEFORE mark other checked
    other._checked = true;

    // except custom error message
    if (other._custom_message != NULL) {
        // do NOT use strdup to make sure new/delete everywhere.
        size_t len = ::strlen(other._custom_message);
        char *copied = new char[len + 1]; // +1 for null terminator
        this->_custom_message = copied;
        ::memcpy(copied, other._custom_message, len + 1);
    }
    return *this;
}

inline w_rc_t::w_rc_t(const w_rc_t &other, const char* filename, uint32_t linenum, const char* more_custom_message) {
    // Invariant: if w_error_ok, no more processing
    if (other._error_code == w_error_ok) {
        this->_error_code = w_error_ok;
        return;
    }

    operator=(other);
    // augment stacktrace
    if (_stack_depth != 0 && _stack_depth < MAX_RCT_STACK_DEPTH) {
        _filenames[_stack_depth] = filename;
        _linenums[_stack_depth] = linenum;
        ++_stack_depth;
    }
    // augment custom error message
    if (more_custom_message != NULL) {
        append_custom_message(more_custom_message);
    }
#if W_DEBUG_LEVEL>=5
    std::cout << "Error augmented: " << *this << std::endl;
#endif //W_DEBUG_LEVEL>=3
}

inline w_rc_t::~w_rc_t() {
    // Invariant: if w_error_ok, no more processing
    if (_error_code == w_error_ok) {
        return;
    }
#if W_DEBUG_LEVEL>0
    // We output warning if some error code is not checked , but we don't do so in release mode.
    verify();
#endif //  W_DEBUG_LEVEL>0
    if (_custom_message != NULL) {
        delete[] _custom_message;
        _custom_message = NULL;
    }
}

inline void w_rc_t::append_custom_message(const char* more_custom_message) {
    // Invariant: if w_error_ok, no more processing
    if (_error_code == w_error_ok) {
        return;
    }
    // augment custom error message
    size_t more_len = ::strlen(more_custom_message);
    if (_custom_message != NULL) {
        // concat
        size_t cur_len = ::strlen(_custom_message);
        char *copied = new char[cur_len + more_len + 1];
        _custom_message = copied;
        ::memcpy(copied, _custom_message, cur_len);
        ::memcpy(copied + cur_len, more_custom_message, more_len + 1);
    } else {
        // just put the new message
        char *copied = new char[more_len + 1];
        _custom_message = copied;
        ::memcpy(copied, more_custom_message, more_len + 1);
    }
}

inline bool w_rc_t::is_error() const {
    _checked = true;
    return _error_code != w_error_ok;
}

inline w_error_codes w_rc_t::err_num() const {
    _checked = true;
    return _error_code;
}

inline const char* w_rc_t::get_message() const {
    assert(_error_code < w_error_count);
    return w_error_message(_error_code);
}

inline const char* w_rc_t::get_custom_message() const {
    // Invariant: if w_error_ok, no more processing
    if (_error_code == w_error_ok) {
        return NULL;
    }
    return _custom_message;
}

inline uint16_t w_rc_t::get_stack_depth() const {
    // Invariant: if w_error_ok, no more processing
    if (_error_code == w_error_ok) {
        return 0;
    }
    return _stack_depth;
}

inline uint16_t w_rc_t::get_linenum(uint16_t stack_index) const {
    // Invariant: if w_error_ok, no more processing
    if (_error_code == w_error_ok) {
        return 0;
    }
    assert(stack_index < _stack_depth);
    return _linenums[stack_index];
}

inline const char* w_rc_t::get_filename(uint16_t stack_index) const {
    // Invariant: if w_error_ok, no more processing
    if (_error_code == w_error_ok) {
        return NULL;
    }
    assert(stack_index < _stack_depth);
    return _filenames[stack_index];
}

inline void w_rc_t::verify() const {
    // Invariant: if w_error_ok, no more processing
    if (_error_code == w_error_ok) {
        return;
    }
    if (!_checked) {
        std::cerr << "WARNING: Return value is not checked. w_rc_t must be checked before deallocation." << std::endl;
        std::cerr << "Error detail:" << *this << std::endl;
    }
}

inline void w_rc_t::fatal() const {
    // Invariant: if w_error_ok, no more processing
    if (_error_code == w_error_ok) {
        return;
    }
    std::cerr << "FATAL: Unexpected error happened. Will exit." << std::endl;
    std::cerr << "Error detail:" << *this << std::endl;
    assert(false);
    std::cout.flush();
    std::cerr.flush();
    std::abort();
}

#endif // W_RC_H
