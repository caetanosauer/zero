#!/usr/bin/ruby

## 
## (c) Copyright 2014, Hewlett-Packard Development Company, LP
## 

if ARGV.length == 0 then
  $stderr.puts "summarize_results.rb: usage: #_of_transactions [#_of_ transactions_multiplier]"
  exit 1
end

# each experiment runs this many transactions:
transactions = ARGV.collect { |x| x.to_i }.inject(1,:*)


r = {}
`grep elapsed tpcc_results/res* | sort`.split(/\n/).each do |result|
  type = result.split(/\./)[1..3].join(",")
  fail unless result =~ /elapsed time=([\d\.]+) sec/
  r[type] ||= []
  r[type] += [$1.to_f]
end


puts "lock,lock,kind,transactions per second (mean),95% confidence interval for (TPS) high, 95% low"
r.keys.sort.each do |type|
  times = r[type]
  next unless times.length > 0

  # we compute arithmetic mean on *times*, not rates! (= harmonic mean on rates)
  n = times.length
  m = times.inject(0.0,:+)/n

  # as standard deviation doesn't make sense for harmonic means,
  # instead calculate 95% confidence interval:
  variance = times.collect { |x| (x-m)**2 }.inject(0.0,:+)/(n-1)
  std = Math.sqrt(variance)
  lower  = m - 1.96*std/Math.sqrt(n)
  higher = m + 1.96*std/Math.sqrt(n)
  # Here, have 95% probability that actual underlying arithmetic time
  # mean is in [lower..higher]

  # If two 95% confidence intervals do not overlap, then there is at
  # most a 5% chance that their underlying processes have the same
  # mean; i.e., there is a significant result that their underlying
  # means differ.

  puts "#{type},#{transactions/m},#{transactions/higher},#{transactions/lower}"
end
