import numpy as np

np.random.seed(1001)
np.random.rand(3, 2)

current_random_state = np.random.get_state()
print(np.random.rand(3, 2))
np.random.set_state(current_random_state)

print(np.random.rand(3, 2)) # the same as above print