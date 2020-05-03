#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>

#define MAX_DATA_COUNT 2000
#define THREAD_MAX 50


int *data_array;
int num_elements;
int num_threads;

void merge(int low, int mid, int high)
{
	int *left;
	int *right;
	int n1, n2;
	int i, j, k;

	left = malloc((mid-low+1) * sizeof(int));
	right = malloc((high-mid) * sizeof(int));

	n1 = mid - low + 1;
	n2 = high - mid;

	for (i = 0; i < n1; i++)
		left[i] = data_array[i + low];

	for (i = 0; i < n2; i++)
		right[i] = data_array[i + mid + 1];

	k = low;
	i = j = 0;

	while (i < n1 && j < n2) {
		if (left[i] <= right[j])
			data_array[k++] = left[i++];
		else
			data_array[k++] = right[j++];
	}

	while (i < n1)
		data_array[k++] = left[i++];

	while (j < n2)
		data_array[k++] = right[j++];
	free(left);
	free(right);
}

void merge_sort(int low, int high)
{
	int mid;

	if (low < high) {
		mid = (low + high)/2;
		merge_sort(low, mid);
		merge_sort(mid + 1, high);
		merge(low, mid, high);
	}
}

void* merge_sort_thread(void *arg)
{
	int i;
	int thread_num;
	int low, high, mid;

	thread_num = (unsigned int)arg;

	low = thread_num * (num_elements/num_threads);
	high = ((thread_num + 1) * num_elements/num_threads) - 1;

	merge_sort(low, high);
#ifdef DEBUG
	printf("Thread: %d Megesort %d to %d\n", thread_num, low, high);
	for (i=low;i<=high;i++)
		printf("%d*", data_array[i]);
	printf("\n");
#endif
}

int main(int argc, char *argv[])
{
	int i;
	int low, mid, high;
	pthread_t thread[THREAD_MAX];
	FILE *fp;
	int min;

	if (argc != 4) {
		printf("Usage: sort datafile min num_threads\n");
		exit(1);
	} 

	fp = fopen(argv[1], "r");
	if (fp == NULL) {
		perror("fopen");
		exit(1);
	}

	min = atoi(argv[2]);
	num_threads = atoi(argv[3]);

	data_array = malloc(MAX_DATA_COUNT);

	for (i = 0; i < MAX_DATA_COUNT; i++)
		if (fscanf(fp, "%d", &data_array[i]) == EOF)
			break;
	num_elements = i;

	if (num_elements <= min)
		num_threads = 2;

#ifdef DEBUG
	printf("Sorting file %s\n", argv[1]);
	printf("Num data elements %d num of threads %d\n", 
			num_elements, num_threads);
	for (i = 0; i < num_elements; i++)
		printf("%d ", data_array[i]);
	printf("\n");
#endif

	for (i = 0; i < num_threads; i++)
		pthread_create(&thread[i], NULL, merge_sort_thread, (void *)i);

	for (i = 0; i < num_threads; i++)
		pthread_join(thread[i], NULL);

	merge_sort(0, num_elements-1);

	for (i = 0; i < num_elements; i++)
		printf("%d ", data_array[i]);
	printf("\n");
}
