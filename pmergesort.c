#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>
#include <time.h>

#define MAX_DATA_COUNT 2000
#define THREAD_MAX 50


int *data_array;	// Contains data elements to be sorted - assumed to be integers
int num_elements; // Number of elements to be sorted
int num_threads;	// Number of threads created for parallel sorting

void merge(int low, int mid, int high)	// Merge sort
{
	int *left;	// Temporary array for left side elements
	int *right; // Temporary array for right side elements
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

void merge_sort(int low, int high)	// Recursively divides all the numbers into two halves and sorts them individually using merge()
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

	thread_num = (unsigned int)arg;	// Thread number that is executing this function

	// This thread sorts elements from low to high
	// data_array is divided into equal parts for each thread
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
	clock_t t1, t2;
	FILE *fp;
	int min;

	if (argc != 4) {
		printf("Usage: sort datafile min num_threads\n");	// Message displayed if wrong arguments are used
		exit(1);
	}

	fp = fopen(argv[1], "r");		// Getting file descriptor for input
	if (fp == NULL) {
		perror("fopen");
		exit(1);
	}

	min = atoi(argv[2]);		// Up to this data size only 2 threads will be created
	num_threads = atoi(argv[3]);		// Number of threads to be created if size is greater than min

	data_array = malloc(MAX_DATA_COUNT);

	for (i = 0; i < MAX_DATA_COUNT; i++)
		if (fscanf(fp, "%d", &data_array[i]) == EOF)	// Reads the numbers from input file
			break;
	num_elements = i;		// Gets count of number of elements

	if (num_elements <= min)
		num_threads = 2;

#ifdef DEBUG	// Displays more information about how threads are functioning
	printf("Sorting file %s\n", argv[1]);
	printf("Num data elements %d num of threads %d\n",
			num_elements, num_threads);
	for (i = 0; i < num_elements; i++)
		printf("%d ", data_array[i]);
	printf("\n");
#endif

	t1 = clock();

	for (i = 0; i < num_threads; i++)
		pthread_create(&thread[i], NULL, merge_sort_thread, (void *)i);	// Creation of threads - executes merge_sort_thread()

	for (i = 0; i < num_threads; i++)
		pthread_join(thread[i], NULL);	// Joining threads

	merge_sort(0, num_elements-1);

	t2 = clock();

	for (i = 0; i < num_elements; i++)
		printf("%d ", data_array[i]);
	printf("\n");

	printf("Time taken is : %f\n", (t2-t1)/(double)CLOCKS_PER_SEC);
}
