my_tuple = (1, 'hello', 3.5)  # Heterogeneous data, fixed size
print(my_tuple[1])

import numpy as np
my_array = np.array([1, 2, 3])
print(my_array)

dict = {}
dict['jiamian'] = 'value'
dict[1] = 2

print(dict)

three_dim_array = [[[1], [2]], [[2], [3]], [[3], [4]]]
print(len(three_dim_array))
print(np.shape(three_dim_array))