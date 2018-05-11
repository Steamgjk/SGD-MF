#include <iostream>

#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <signal.h>
#include <unistd.h>
#include <stdlib.h>
#include <assert.h>
#include <stdio.h>
#include <string.h>
#include <cmath>
#include <time.h>
#include <vector>
#include <list>
#include <thread>
#include <chrono>
#include <algorithm>
#include <mutex>
#include <iostream>
#include <fstream>
#include <sys/time.h>
#include <map>

#define N 71567
#define M 65133
#define K  40 //主题个数
int ITER_NUM  = 2000;
int PORTION_NUM = 8;
double P[N][K];
double Q[K][M];
//#define TEST_NAME "./test_out.txt"
#define FILE_NAME "./mdata/traina-"
#define TEST_NAME "./mdata/testa-"

using namespace std;
map<long, double> TestMap;
std::vector<long> hashs;
std::vector<double> rts;
void LoadTestRating()
{
    char fn[100];
    for (int i = 0; i < 6 ; i++)
    {
        sprintf(fn, "%s%d", TEST_NAME, i);
        ifstream ifs(fn);
        if (!ifs.is_open())
        {
            printf("fail to open the file %s\n", TEST_NAME);
            exit(-1);
        }
        int cnt = 0;
        int temp = 0;
        long hash_idx = 0;
        double ra = 0;
        hash_idx = -1;
        while (!ifs.eof())
        {
            ifs >> hash_idx >> ra;
            //TestMap.insert(pair<long, double>(hash_idx, ra));
            if (hash_idx >= 0)
            {
                hashs.push_back(hash_idx);
                rts.push_back(ra);
                cnt++;
                if (cnt % 10000 == 0)
                {
                    printf("cnt = %d\n", cnt );
                }
            }

        }
    }

}
double CalcRMSE()
{
    printf("calc RMSE...\n");
    double rmse = 0;
    int cnt = 0;
    map<long, double>::iterator iter;
    int positve_cnt = 0;
    int negative_cnt = 0;
    double sss = 0;
    double avg = 0;
    printf("hehere\n");
    for (int i = 0; i < hashs.size(); i++ )
    {
        printf("[%d] %ld\n", i, hashs[i] );
    }
    /*
    //for (iter = TestMap.begin(); iter != TestMap.end(); iter++)
    for (int ss = 0; ss < hashs.size(); ss++)
    {
        //long real_hash_idx = iter->first;
        long real_hash_idx = hashs[ss];
        double rate = rts[ss];

        long row_idx = real_hash_idx / M;
        long col_idx = real_hash_idx % M;
        double sum = 0;
        printf("real_hash_idx=%ld \n", real_hash_idx );

        for (int k = 0; k < K; k++)
        {
            sum += P[row_idx][k] * Q[k][col_idx];
            //printf("%lf  %lf\n", P[row_idx][k], Q[k][col_idx]);
        }
        if (sum > iter->second)
        {
            positve_cnt++;
        }
        else
        {
            negative_cnt++;

        }
        rmse += (sum - rts[ss] ) * (sum - rts[ss]);

        cnt++;
    }

    rmse /= cnt;
    rmse = sqrt(rmse);
    printf("positve_cnt=%d negative_cnt=%d rmse=%lf\n", positve_cnt, negative_cnt, rmse );
    **/
    return rmse;
}
int main(int argc, const char * argv[])
{
    ofstream ofs("./rima.txt", ios::trunc);

    if (argc >= 2)
    {
        ITER_NUM = atoi(argv[1]);
    }
    if (argc >= 3)
    {
        PORTION_NUM = atoi(argv[2]);
    }

    char filename[100];
    for (int i = 0; i < ITER_NUM; i += 10)
    {
        int row_idx = 0;
        int col_idx = 0;
        for (int j = 0 ; j < PORTION_NUM; j++)
        {
            sprintf(filename, "./Rtrack/Pblock-%d-%d", i, j);
            ifstream ifs(filename, ios::in | ios::out);
            if (!ifs.is_open())
            {
                printf("fail to open %s\n", filename);
                getchar();
            }

            while (!ifs.eof())
            {
                for (int kk = 0; kk < K; kk++)
                {
                    ifs >> P[row_idx][kk];
                }
                row_idx++;
            }

            printf("%s read\n", filename );
            sprintf(filename, "./Rtrack/Qblock-%d-%d", i, j);
            ifstream ifs1(filename, ios::in | ios::out);
            if (!ifs1.is_open())
            {
                printf("fail to open %s\n", filename);
                getchar();
            }
            double temp;
            while (!ifs1.eof())
            {

                for (int kk = 0; kk < K; kk++)
                {
                    ifs1 >> Q[kk][col_idx];

                }

                col_idx++;
            }
            printf("%s read\n", filename );

        }
        //double rmse = CalcRMSE();
        LoadTestRating();
        printf("Sz T  %ld\n", hashs.size() );
        double rmse = 0;
        /*
        for (int i = 0; i < hashs.size(); i++ )
        {
            printf("[%d] %ld\n", i, hashs[i] );
        }
        **/

        printf("%ld \n", hashs.size() );
        getchar();
        ofs << rmse << endl;
        printf("%d\t%lf\n", i, rmse );
    }
    return 0;


}


// 1.7