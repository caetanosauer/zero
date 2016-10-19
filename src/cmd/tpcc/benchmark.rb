#!/usr/bin/ruby

## 
## (c) Copyright 2014, Hewlett-Packard Development Company, LP
## 


## 
## Parse arguments to generate tpcc_full command:
## 

if ARGV.length == 0 or ARGV[0] !~ /^\d+$/ then
  $stderr.puts "benchmark.rb: usage: #_of_runs <tpcc_full_parameter>*"
  exit 1
end

runs                  = ARGV.shift.to_i

workers               = nil
transactions_per_core = nil
ARGV.each_with_index do |arg, i|
  case arg
  when "--workers"
    workers = ARGV[i+1].to_i
    
  when "--transactions"
    transactions_per_core = ARGV[i+1].to_i
  end
end


parameters = "--pin_numa --verbose_level=0 " + ARGV.join(" ")

if not workers then
  workers = 12
  parameters += " --workers #{workers} "
end
if not transactions_per_core then
  transactions_per_core = 20000
  parameters += " --transactions #{transactions_per_core} "
end

command    = "numactl --interleave=all ./tpcc_full #{parameters}"

puts "Using  runs: #{runs}  cores: #{workers}  transactions per core: #{transactions_per_core}"
puts


## 
## Setting up starting state:
## 

puts "Creating TPC-C database image..."

system('mkdir -p /dev/shm/`whoami`/foster')
system('rm -rf /dev/shm/`whoami`/foster/*')
system('mkdir -p /dev/shm/`whoami`/foster/log')
system("./tpcc_load --nolock > /dev/null")  # redirect for debug mode

system('mkdir -p /dev/shm/`whoami`/backup/')
system('rm -rf /dev/shm/`whoami`/backup/*')
system('cp -r /dev/shm/`whoami`/foster/data /dev/shm/`whoami`/foster/log /dev/shm/`whoami`/backup/')


## 
## Running experiments:
## 

puts "Running experiments..."

transactions = workers * transactions_per_core

times = []
1.upto(runs) do |i|
  system('rm -rf /dev/shm/`whoami`/foster/data /dev/shm/`whoami`/foster/log')
  system('cp -r /dev/shm/`whoami`/backup/data /dev/shm/`whoami`/backup/log /dev/shm/`whoami`/foster/')

  output = `#{command} | grep elapsed`
  fail unless output =~ /elapsed time=([\d\.]+) sec/
  time = $1.to_f

  times.push(time)
  printf("  run %2d: %10.0f transactions per second  [ran for %4.3fs]\n", i, transactions/time, time)
end


## 
## Calculating statistics:
## 

# we compute arithmetic mean on *times*, not rates! (= harmonic mean on rates)
n = times.length
exit 0 unless n>0
m = times.inject(0.0,:+)/n

printf("(Harmonic) mean transactions per core per second = %.1f\n", transactions/m/workers)
puts
printf("(Harmonic) mean transactions per second = %.1f\n", transactions/m)


# as standard deviation doesn't make sense for harmonic means,
# instead calculate 95% confidence interval:
variance = times.collect { |x| (x-m)**2 }.inject(0.0,:+)/(n-1)
exit 0 unless n>1
std    = Math.sqrt(variance)
lower  = m - 1.96*std/Math.sqrt(n)
higher = m + 1.96*std/Math.sqrt(n)
# Here, have 95% probability that actual underlying arithmetic time
# mean is in [lower..higher]

# If two 95% confidence intervals do not overlap, then there is at
# most a 5% chance that their underlying processes have the same
# mean; i.e., there is a significant result that their underlying
# means differ.

printf("95%% confidence interval (TPS): [%.1f .. %.1f]\n", transactions/higher, transactions/lower)
