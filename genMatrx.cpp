#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <iostream>
#include <fstream>
using namespace std;
#define N 16
#define M 16
#define K 2
double R[N][M];
double P[N][K];
double Q[K][N];
int main()
{
	srand(time(0));
	ofstream Pofs("./Pmtx.txt", ios::trunc);
	ofstream Qofs("./Qmtx.txt", ios::trunc);
	for (int i = 0; i < N; i++)
	{
		for (int j = 0; j < K; j++)
		{
			P[i][j] = ((double)(rand() % 1000)) / 100;
			Pofs << P[i][j] << " ";
		}
		Pofs << endl;
	}
	for (int i = 0; i < K; i++)
	{
		for (int j = 0; j < M; j++)
		{
			Q[i][j] = ((double)(rand() % 1000)) / 100;
			Qofs << Q[i][j] << " ";
		}
		Qofs << endl;
	}
	cout << "finish P and Q" << endl;
	for (int i = 0; i < N; i++)
	{
		for (int j = 0; j < M; j++)
		{
			R[i][j] = 0;
			for (int k = 0; k < K; k++)
			{
				R[i][j] += P[i][k] * Q[k][j];
			}
		}
	}

	ofstream ofs("./mtx.txt", ios::trunc);

	for (int i = 0; i < N; i++)
	{
		for (int j = 0; j < M; j++)
		{
			ofs << R[i][j] << " ";
		}
		ofs << endl;
	}
	cout << "finish" << endl;
	getchar();

	ifstream ifs("./mtx.txt");//请更换成自己的文件名

	char tmp;
	while (!ifs.eof())
	{
		for (int i = 0; i < N; i++)
		{
			for (int j = 0; j < M; j++)
			{
				ifs >> R[i][j];
				printf("%lf\n", R[i][j] );
			}
			printf("\n");
		}
	}

}