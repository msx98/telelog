from typing import *
import random
import time


def collect_list(
	it,
	max_elements: int = 1000,
	*,
	delay: float = 0.00,
	random_lower_limit: float = 0,
	random_upper_limit: float = 0.1,
) -> list:
	l = list()
	for i in it:
		time.sleep(delay + random.uniform(random_lower_limit, random_upper_limit))
		l.append(i)
		if max_elements and len(l) >= max_elements:
			break
	return l