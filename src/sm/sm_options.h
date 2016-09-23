/*
 * (c) Copyright 2014-, Hewlett-Packard Development Company, LP
 */

#ifndef SM_OPTIONS_H
#define SM_OPTIONS_H

#include <stdint.h>
#include <string>
#include <map>

/**
 * \brief Start-up parameters for the storage engine.  See \ref OPTIONS.
 * \ingroup OPTIONS
 * \details
 *   This class represents the entire set of parameters our storage engine needs
 * while start-up. It consists of integer parameters, such as buffer pool size,
 * boolean parameters and string parameters, such as log drive path. They are exposed simply as a map
 * of parameter name and the value.
 * NOTE This class is header-only. No need to link to anything.
 */
class sm_options {
public:
    /** empty constructor. */
    sm_options();

    /** copy constructor. */
    sm_options(const sm_options& other);

    /**
     * Returns the value of the specified integer start up parameter.
     * @param[in] param_name name of the parameter
     * @param[in] default_value value to return if the parameter does not exist.
     * @return the value of the specified integer start up parameter, or the default value if it doesn't exist.
     */
    int64_t get_int_option(const std::string& option_name, int64_t default_value) const;

    /**
     * Returns the value of the specified boolean start up parameter.
     * @param[in] param_name name of the parameter
     * @param[in] default_value value to return if the parameter does not exist.
     * @return the value of the specified boolean start up parameter, or the default value if it doesn't exist.
     */
    bool get_bool_option(const std::string& option_name, bool default_value) const;

    /**
     * Returns the value of the specified string start up option.
     * @param[in] option_name name of the option
     * @param[in] default_value value to return if the option does not exist.
     * @return the value of the specified string start up option, or the default value if it doesn't exist.
     */
    const std::string& get_string_option(const std::string& option_name, const std::string& default_value) const;

    /**
     * Sets the value of the specified integer start up option.
     * @param[in] option_name name of the option
     * @param[in] default_value value of the option.
     */
    void set_int_option(const std::string& option_name, int64_t value);

    /**
     * Sets the value of the specified boolean start up option.
     * @param[in] option_name name of the option
     * @param[in] default_value value of the option.
     */
    void set_bool_option(const std::string& option_name, bool value);

    /**
     * Sets the value of the specified string start up option.
     * @param[in] option_name name of the option
     * @param[in] default_value value of the option.
     */
    void set_string_option(const std::string& option_name, const std::string& value);

private:
    /** All integer options, key=option name. */
    std::map<std::string, int64_t> _int_options;

    /** All boolean options, key=option name. */
    std::map<std::string, bool> _bool_options;

    /** All string options, key=option name. */
    std::map<std::string, std::string> _string_options;

    // so far no floating point options. we can use integer with scales.
};


// old name in Shore-MT just for compatibility.
// typedef sm_options option_group_t;

inline sm_options::sm_options() {}
inline sm_options::sm_options(const sm_options& other)
    : _int_options(other._int_options), _bool_options(other._bool_options), _string_options(other._string_options) {
}

template <typename V>
const V& get_option_with_default (
    const std::map<std::string, V> &the_map, const std::string& option_name, const V &default_value) {
    typename std::map<std::string, V>::const_iterator it = the_map.find(option_name);
    if (it == the_map.end()) {
        return default_value;
    } else {
        return it->second;
    }
}

inline int64_t sm_options::get_int_option(const std::string& option_name, int64_t default_value) const {
    return get_option_with_default<int64_t>(_int_options, option_name, default_value);
}
inline bool sm_options::get_bool_option(const std::string& option_name, bool default_value) const {
    return get_option_with_default<bool>(_bool_options, option_name, default_value);
}
inline const std::string& sm_options::get_string_option(const std::string& option_name, const std::string& default_value) const {
    return get_option_with_default<std::string>(_string_options, option_name, default_value);
}

inline void sm_options::set_int_option(const std::string& option_name, int64_t value) {
    _int_options[option_name] = value;
}
inline void sm_options::set_bool_option(const std::string& option_name, bool value) {
    _bool_options[option_name] = value;
}
inline void sm_options::set_string_option(const std::string& option_name, const std::string& value) {
    _string_options[option_name] = value;
}

#endif // SM_OPTIONS_H
