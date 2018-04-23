#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <iostream>
#include <fstream>
#include <map>
using namespace std;

#define FILE_NAME "./netflix_mtx.txt"
#define N  17770 // row number
#define M  2649429 //col number

#define OUT_NAME "./netflix_row.txt"
map<long, double> mm;
int main()
{

	ifstream ifs(FILE_NAME);
	ofstream ofs(OUT_NAME, ios::trunc);
	if (!ifs.is_open())
	{
		printf("fail to open the file %s\n", FILE_NAME);
		exit(-1);
	}
	int cnt = 0;
	double temp = 0;
	for (long int i = 0; i < N; i++)
	{
		for (long int j = 0; j < M; j++)
		{
			ifs >> temp;
			if (temp > 0)
			{
				long hash_idx = i * M + j;
				ofs << hash_idx << " " << temp << endl;
				cnt++;
				if (hash_idx < 0)
				{
					printf("err  hash_idx=%ld\n", hash_idx );
					getchar();
				}
			}
		}
		if (i % 1000 == 0)
		{
			printf("i=%ld\n", i );
		}
	}

	printf("cnt=%d sizeof(long)=%ld\n", cnt, sizeof(long));


	printf("mmsz=%ld\n", mm.max_size());
}