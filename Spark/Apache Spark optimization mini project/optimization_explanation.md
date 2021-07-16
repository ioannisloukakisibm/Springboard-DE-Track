The original code took the time below to run:
760 ms ± 17.4 ms per loop (mean ± std. dev. of 7 runs, 1 loop each)

Adding repartition of the original datasets
633 ms ± 19.3 ms per loop (mean ± std. dev. of 7 runs, 1 loop each)

Since one of the data sets will be used twice it was worth caching it, something that dropped processing time significantly
246 ms ± 39.4 ms per loop (mean ± std. dev. of 7 runs, 1 loop each)

Turned on adaptive SQL query for additional performance efficiencies. 
205 ms ± 5.86 ms per loop (mean ± std. dev. of 7 runs, 1 loop each)
