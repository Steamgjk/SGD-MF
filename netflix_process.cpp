#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <iostream>
#include <fstream>
#include <string>
using namespace std;
#define N 10000
#define M 10000
#define K 2
#define MOVIE_NUM 17770
#define CUSTOMER_NUM 2649429
double user_rates[CUSTOMER_NUM + 10];
int main()
{
	char filename[100];
	int temp;
	int customer_id;
	char datetime[100];
	int rate;
	char comma;
	int movie_id;
	char line[512];
	//ofstream ofs("netflix_mtx.txt", ios::trunc);
	ofstream ofs("netflix_rown.txt", ios::trunc);

	for (long i = 1; i <= MOVIE_NUM; i++)
	{
		for (long j = 0; j <= CUSTOMER_NUM; j++)
		{
			user_rates[j] = 0;
		}
		sprintf(filename, "./training_set/mv_%.7ld.txt", i);

		ifstream ifs(filename);//请更换成自己的文件名
		//ifs >> movie_id >> comma;
		if (!ifs.is_open())
		{
			cout << "Error opening file"; exit (1);
		}
		ifs >> movie_id >> comma;
		printf("filename=%s movie_id=%d\n", filename, movie_id);
		while (!ifs.eof())
		{
			ifs >> customer_id >> comma >> rate >> comma >> datetime;
			//cout << customer_id << "!" << comma << "!" << rate << "!" << datetime << endl;
			user_rates[customer_id] = rate;
			//getchar();
		}
		for (long j = 1; j <= CUSTOMER_NUM; j++ )
		{
			//ofs << user_rates[j] << " ";
			if (i == 10000 && j == 200206)
			{
				printf("i=%ld j=%ld hash_id=%ld userr=%lf\n", i, j, (i - 1)*CUSTOMER_NUM + j - 1,  user_rates[j] );
			}
			if (user_rates[j] > 0)
			{
				ofs << (i - 1)*CUSTOMER_NUM + j - 1 << " " << user_rates[j] << endl;
				if (i == 10000 && j == 200206)
				{
					printf("i=%ld j=%ld hash_id=%ld userr=%lf\n", i, j, (i - 1)*CUSTOMER_NUM + j - 1,  user_rates[j] );
				}

			}

		}
		ofs << endl;
	}




}