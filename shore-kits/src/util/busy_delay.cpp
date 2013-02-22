
/** @file busy_delay.c
 *
 *  @brief Implements busy_delay operations. All exported
 *  functions return 0 on success.
 *
 *  @author Naju Mancheril (ngm)
 */
#include "util/busy_delay.h" /* for prototypes */
#include <cstdlib>      /* for NULL, malloc, free */

#ifdef __GCC
#include <cstring>
#else
#include <string.h> /* On Sun's CC <string.h> defines memset,
                       <cstring> doesn't */
#endif

#include <cstring>      /* for memset */
#include <math.h>       /* for log */
#include <cassert>      /* for assert */
#include <sys/time.h>   /* for gettimeofday */
#include "util/trace.h" /* for TRACE */



/* data structures */

static double _NAN;
static double MS_PER_SEC  = 1000.0;
static double MS_PER_USEC = 0.001;
static int NUM_ITERATIONS_PER_MS = -1;
static int NUM_RUNS_PER_ITERATION_COUNT = 10;



/* helper functions */

static void iteration(void);
static int compute_iterations_per_ms(int min_pow, int num_pow);



/* definitions of exported functions */

/**
 *  @brief Initialize (calibrate) busy_delay module.
 *
 *  @return 0 on success. Non-zero otherwise.
 */
int busy_delay_init(void)
{
  /* error checking */
  assert(NUM_ITERATIONS_PER_MS == -1);

  /* initialize other constants */
  _NAN = log((double)-1);

  /* calibrate */
  int min_pow = 10;
  int num_pow = 10;
  int num = compute_iterations_per_ms(min_pow, num_pow);
  if (num < 0)
    return -1;

  TRACE(0&TRACE_ALWAYS, "Computed %d iterations per ms\n", num);
  NUM_ITERATIONS_PER_MS = num;
  return 0;
}


/**
 *  @brief Delay for the specified number of milliseconds.
 */
void busy_delay_ms(int ms)
{
  /* error checking */
  assert(NUM_ITERATIONS_PER_MS > 0);

  int m;
  for (m = 0; m < ms; m++)
  {
    int i;
    for (i = 0; i < NUM_ITERATIONS_PER_MS; i++)
      iteration();
  }
}


/**
 *  @brief Delay for the specified number of microseconds.
 */
void busy_delay_us(int us)
{
  /* error checking */
  assert(NUM_ITERATIONS_PER_MS > 0);

  int num_iterations_per_us = NUM_ITERATIONS_PER_MS / 1000;
  int m;
  for (m = 0; m < us; m++)
  {
    int i;
    for (i = 0; i < num_iterations_per_us; i++)
      iteration();
  }
}



/* definitions of helper functions */

/**
 * @brief Do a single iteration worth of work.
 */
static void iteration(void)
{
  /* Do a small amount of busy work which the compiler will not
     optimize away. */
  static int _temp = 0;
  volatile int* _temp_ptr = &_temp;
  *_temp_ptr = *_temp_ptr + 1;
}


static double compute_time_ms(int num_iter)
{
  assert(num_iter > 0);

  struct timeval start, end;
  memset(&start, 0, sizeof(struct timeval));
  memset(&end,   0, sizeof(struct timeval));

  /* record time */
  if (gettimeofday(&start, NULL))
    return _NAN;

  int i;
  for (i = 0; i < num_iter; i++)
    iteration();

  /* record time */
  if (gettimeofday(&end, NULL))
    return _NAN;

  /* convert struct timeval into doubles */
  double start_time =
    start.tv_sec * MS_PER_SEC + start.tv_usec * MS_PER_USEC;
  double end_time =
    end.tv_sec * MS_PER_SEC + end.tv_usec * MS_PER_USEC;
  
  return end_time - start_time;
}


static int compute_iterations_per_ms(int min_pow, int num_pow)
{
  int sum_iter = 0;
  double sum_time = 0.0;

  int i;
  int p = min_pow;
  for (i = 0; i < num_pow; i++)
  {
    int num_iter = 1 << p;
    assert(num_iter > 0);

    /* Compute the time to run 'num_iter' iterations. To avoid
       variances, we repeat NUM_RUNS_PER_ITERATION_COUNT times and
       take average (arithmetic mean). */
    double sum = 0.0;
    int r;
    for (r = 0; r < NUM_RUNS_PER_ITERATION_COUNT; r++)
    {
      /* compute time to run 'num_iter' iterations */
      double t = compute_time_ms(num_iter);
      if (isnan(t))
      {
        /* error checking ... check for _NAN */
        TRACE(0&TRACE_DEBUG, "compute_time_ms(%d) return NAN\n", num_iter);
        return -1;
      }

      sum += t;
    }

    double tavg = sum / NUM_RUNS_PER_ITERATION_COUNT;
    TRACE(0&TRACE_DEBUG,
          "Computed time of %lf for %d iterations\n",
          tavg,
          num_iter);
    
    sum_iter += num_iter;
    sum_time += tavg;
    
    p++;
  }

  return (int)( sum_iter / sum_time );
}
