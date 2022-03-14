cimport numpy as np
from libcpp.vector cimport vector
from libcpp.map cimport map as map

cdef np.ndarray entry
cdef np.ndarray position

cdef void start_analysis(
  np.ndarray[np.float64_t, ndim=2] price_, 
  map[int, int] pos2price_, 
  int nstocks, int window_, int window_step_)

cdef void record_date(int d)

cdef void record_entry(int sid, double position_sid)

cdef void record_exit(int sid)

