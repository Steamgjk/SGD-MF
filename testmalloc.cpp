#include <stdlib.h>
#include <stdio.h>
using namespace std;
#define ELE_NUM 4130680370
int main()
{
	double *R = (double*)malloc(ELE_NUM * sizeof(double));
	printf("R=%p\n", R );
}