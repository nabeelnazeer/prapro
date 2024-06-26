"""

Create a 1000 x 1000 random numpy array
Measure how long the creation takes
Convert the array into bytes
Recreate the array from the bytes

"""
import numpy as np
import time

start_time = time.time()
A = np.random.randint((1000,1000))
time_taken = time.time() - start_time

print("time taken : ",time_taken," seconds" )

ar_bytes = A.tobytes()

start_time = time.time()
new_ar = np.frombuffer(ar_bytes, dtype = A.dtype).reshape(A.shape)
conv_time_taken = time.time() - start_time
print("time taken : ",conv_time_taken," seconds" )


