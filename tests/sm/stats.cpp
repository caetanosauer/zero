#include "stats.h"

const uint32_t Stats::stats_version_major = 6;
const uint32_t Stats::stats_version_minor = 1;

/* Each type has the same probability.
 * For any type, 241 length possibilities: [80, 320].
 * For any type, all lengths have the same probability: 1 / 241
 */
Stats::Stats()
    :
    probType(logrec_t::t_max_logrec, (float) 1.0 / logrec_t::t_max_logrec),
    probLengthIndex(logrec_t::t_max_logrec, vector<float>(241, (float)1.0 / 241)),
    lengthIndexToLength(logrec_t::t_max_logrec, vector<uint32_t>(241,0)),
    gen(1729), /* seed is 1729, as a good omen */
    typeDist(probType)
{
    for(int i=0; i<logrec_t::t_max_logrec; i++) {
        for(int j=0; j<241; j++) {
            lengthIndexToLength[i][j] = 80+j;
        }
    }
}

/* This constructor must be called by all subclasses */
Stats::Stats(unsigned _lengthDim)
    :
    probType(logrec_t::t_max_logrec, 0),
    probLengthIndex(logrec_t::t_max_logrec, vector<float>(_lengthDim,0.0)),
    lengthIndexToLength(logrec_t::t_max_logrec, vector<uint32_t>(_lengthDim,0))
{
}

/**
 * Generates a random type following a distribution based on the statistics
 * of log records created by the execution of a benchmark.
 */
unsigned Stats::nextType() {
    unsigned type = (typeDist(gen));
    return type;
}

/* Generates a random length given a certain type, following a distribution
 * based on the statisticsof log records created by the execution of a
 * benchmark.
 */
unsigned Stats::nextLength(unsigned _type) {
    boost::random::discrete_distribution<> lengthDist(probLengthIndex[_type]);
    int lengthIndex = lengthDist(gen);
    unsigned length = lengthIndexToLength[_type][lengthIndex];
    return length;
}
