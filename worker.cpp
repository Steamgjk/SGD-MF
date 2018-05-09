//
//  main.cpp
//  linux_socket_api
//
//  Created by bikang on 16/11/2.
//  Copyright (c) 2016年 bikang. All rights reserved.
//

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
using namespace std;

//cnt=15454227 sizeof(long)=8
//#define FILE_NAME "./netflix_row.txt"
//#define TEST_NAME "./test_out.txt"
//#define N  17770 // row number
//#define M  2649429 //col number
//#define K  40 //主题个数

//#define FILE_NAME "./movielen10M_train.txt"
//#define TEST_NAME "./movielen10M_test.txt"
/*
#define FILE_NAME "./traina.txt"
#define TEST_NAME "./testa.txt"
#define N 71567
#define M 65133
#define K  40 //主题个数
**/

#define FILE_NAME "./data/TrainingMap-"
#define TEST_NAME "./data/TestMap-"
#define N 1000000
#define M 1000000
#define K  100 //主题个数

#define WORKER_NUM 1
char* remote_ips[10] = {"12.12.10.18", "12.12.10.18", "12.12.10.18", "12.12.10.18"};
int remote_ports[10] = {4411, 4412, 4413, 4414};

char* local_ips[10] = {"12.12.10.12", "12.12.10.15", "12.12.10.16", "12.12.10.17"};
int local_ports[10] = {5511, 5512, 5513, 5514};



#define ThreshIter 1000
#define SEQ_LEN 5000
#define WORKER_THREAD_NUM 10

struct Block
{
    int block_id;
    int data_age;
    int sta_idx;
    int height; //height
    int ele_num;
    vector<double> eles;
    Block()
    {

    }
    Block operator=(Block& bitem)
    {
        block_id = bitem.block_id;
        data_age = bitem.data_age;
        height = bitem.height;
        eles = bitem.eles;
        ele_num = bitem.ele_num;
        sta_idx = bitem.sta_idx;
        return *this;
    }
    void printBlock()
    {

        printf("block_id  %d\n", block_id);
        printf("data_age  %d\n", data_age);
        printf("ele_num  %d\n", ele_num);
        for (int i = 0; i < eles.size(); i++)
        {
            printf("%lf\t", eles[i]);
        }
        printf("\n");

    }
};
struct Updates
{
    int block_id;
    int clock_t;
    int ele_num;
    vector<double> eles;
    Updates()
    {

    }
    Updates operator=(Updates& uitem)
    {
        block_id = uitem.block_id;
        clock_t = uitem.clock_t;
        ele_num = uitem.ele_num;
        eles = uitem.eles;
        return *this;
    }

    void printUpdates()
    {

        printf("update block_id %d\n", block_id );
        printf("clock_t  %d\n", clock_t);
        printf("ele size %ld\n", ele_num);
        for (int i = 0; i < eles.size(); i++)
        {
            printf("%lf\t", eles[i]);
        }
        printf("\n");

    }
};
struct Block Pblock;
struct Block Qblock;
struct Updates Pupdt;
struct Updates Qupdt;
vector<double> oldP;
vector<double> oldQ;
vector<long> hash_ids;
std::vector<long> rates;

bool canSend = false;
bool hasRecved = false;
int block_seq[SEQ_LEN];

double yita = 0.2;
double theta = 0.05;

int wait4connection(char*local_ip, int local_port);
void sendTd(int send_thread_id);
void recvTd(int recv_thread_id);
//void submf(double *minR, Block& minP, Block& minQ, Updates& updateP, Updates& updateQ,  int minK, int steps = 50, float alpha = 0.0002, float beta = 0.02);
//void submf(Block& minP, Block& minQ, Updates& updateP, Updates& updateQ,  int minK, int steps = 50, float alpha = 0.1, float beta = 0.1);
void submf();
void testhello(int th);

void LoadConfig(char*filename);
void WriteLog(Block&Pb, Block&Qb, int iter_cnt);
void LoadRating();
void LoadTestRating();
void LoadRmatrix(int file_no, map<long, double>& myMap);
void  FilterDataSet(map<long, double>& TestMap, long row_sta_idx, long row_len, long col_sta_idx, long col_len);
double CalcRMSE(map<long, double>& TestMap, Block & minP, Block & minQ);
vector<bool> StartCalcUpdt;
void CalcUpdt(int thread_id);
int thread_id = -1;

struct timeval start, stop, diff;


//double* minR = (double*)malloc(sizeof(double) * Rsz);
map<long, double> RMap;
map<long, double> TestMap;

std::vector<long> hash_for_row_threads[WORKER_THREAD_NUM];
std::vector<double> rates_for_row_threads[WORKER_THREAD_NUM];

std::vector<long> hash_for_col_threads[WORKER_THREAD_NUM];
std::vector<double> rates_for_col_threads[WORKER_THREAD_NUM];

int main(int argc, const char * argv[])
{

    int thresh_log = 900;
    thread_id = atoi(argv[1]);
    if (argc >= 3)
    {
        thresh_log = atoi(argv[2]);
    }
    //LoadRating();
    //LoadTestRating();
    printf("Load Rating Success\n");

    StartCalcUpdt.resize(WORKER_THREAD_NUM);
    for (int i = 0; i < WORKER_THREAD_NUM; i++)
    {
        StartCalcUpdt[i] = false;
    }

    memset(&start, 0, sizeof(struct timeval));
    memset(&stop, 0, sizeof(struct timeval));
    memset(&diff, 0, sizeof(struct timeval));

    std::thread send_thread(sendTd, thread_id);
    send_thread.detach();

    std::thread recv_thread(recvTd, thread_id);
    recv_thread.detach();

    std::vector<thread> td_vec;
    for (int i = 0; i < WORKER_THREAD_NUM; i++)
    {
        //std::thread td(CalcUpdt, i);
        td_vec.push_back(std::thread(CalcUpdt, i));
    }
    //printf("come here\n");
    for (int i = 0; i < WORKER_THREAD_NUM; i++)
    {
        td_vec[i].detach();
        printf("%d  has detached\n", i );
    }



    //double* minR = (double*)malloc(sizeof(double) * 1000);
    int iter_cnt = 0;
    bool isstart = false;
    while (1 == 1)
    {

        if (hasRecved)
        {
            //printf("has Received\n");
            if (!isstart)
            {
                isstart = true;
                gettimeofday(&start, 0);
            }

            //SGD
            int row_sta_idx = Pblock.sta_idx;
            int row_len = Pblock.height;
            int col_sta_idx = Qblock.sta_idx;
            int col_len = Qblock.height;
            int ele_num = row_len * col_len;

            submf();
            iter_cnt++;
            if (iter_cnt == thresh_log )
            {
                gettimeofday(&stop, 0);

                long long mksp = (stop.tv_sec - start.tv_sec) * 1000000 + stop.tv_usec - start.tv_usec;
                printf("itercnt = %d  time = %lld\n", iter_cnt, mksp);
                //WriteLog(Pblock, Qblock, iter_cnt);
                exit(0);
            }

            //free(minR);

            canSend = true;
            hasRecved = false;

        }
    }

}
void testhello(int th)
{
    while (th--)
    {
        printf("test hello\n");
        std::this_thread::sleep_for(std::chrono::milliseconds(1000));
    }
}
void LoadRating()
{
    char fn[100];
    for (int f_no = 0; f_no < 64; f_no++)
    {
        sprintf(fn, "%s%d", FILE_NAME, f_no);
        ifstream ifs(fn);
        if (!ifs.is_open())
        {
            printf("fail to open the file %s\n", fn);
            exit(-1);
        }
        int cnt = 0;
        int temp = 0;
        long hash_idx = 0;
        double ra = 0;
        while (!ifs.eof())
        {
            ifs >> hash_idx >> ra;
            RMap.insert(pair<long, double>(hash_idx, ra));
            cnt++;
            if (cnt % 1000000 == 0)
            {
                printf("cnt = %ld\n", cnt );
            }
        }
    }
}
void LoadTestRating()
{
    char fn[100];
    for (int f_no = 0; f_no < 64; f_no++)
    {
        sprintf(fn, "%s%d", TEST_NAME, f_no);
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
        while (!ifs.eof())
        {
            ifs >> hash_idx >> ra;
            TestMap.insert(pair<long, double>(hash_idx, ra));
            cnt++;
            if (cnt % 10000 == 0)
            {
                printf("cnt = %ld\n", cnt );
            }
        }
    }
}

void LoadRmatrix(int file_no, map<long, double>& myMap)
{
    char fn[100];
    sprintf(fn, "%s%d", FILE_NAME, file_no);
    ifstream ifs(fn);
    if (!ifs.is_open())
    {
        printf("fail to open the file %s\n", fn);
        exit(-1);
    }
    int cnt = 0;
    long hash_idx = 0;
    double ra = 0;
    while (!ifs.eof())
    {
        ifs >> hash_idx >> ra;
        myMap.insert(pair<long, double>(hash_idx, ra));
        cnt++;
        if (cnt % 1000000 == 0)
        {
            printf("cnt = %ld\n", cnt );
        }
    }
}

void WriteLog(Block&Pb, Block&Qb, int iter_cnt)
{
    char fn[100];
    sprintf(fn, "./PS-track/Pblock-%d-%d", iter_cnt, Pb.block_id);
    ofstream pofs(fn, ios::trunc);
    for (int h = 0; h < Pb.height; h++)
    {
        for (int j = 0; j < K; j++)
        {
            pofs << Pb.eles[h * K + j] << " ";
        }
        pofs << endl;
    }
    sprintf(fn, "./PS-track/Qblock-%d-%d", iter_cnt, Qb.block_id);
    ofstream qofs(fn, ios::trunc);
    for (int h = 0; h < Qb.height; h++)
    {
        for (int j = 0; j < K; j++)
        {
            qofs << Qb.eles[h * K + j] << " ";
        }
        qofs << endl;
    }
}


double CalcRMSE(map<long, double>& RTestMap, Block & minP, Block & minQ)
{
    //printf("calc RMSE-1debug...\n");
    double rmse = 0;
    int cnt = 0;
    map<long, double>::iterator iter;
    int positve_cnt = 0;
    int negative_cnt = 0;
    long row_sta_idx = minP.sta_idx;
    long col_sta_idx = minQ.sta_idx;
    for (iter = RTestMap.begin(); iter != RTestMap.end(); iter++)
    {
        long real_hash_idx = iter->first;
        long row_idx = real_hash_idx / M - row_sta_idx;
        long col_idx = real_hash_idx % M - col_sta_idx;
        double sum = 0;

        for (int k = 0; k < K; k++)
        {
            //sum += P[row_idx][k] * Q[k][col_idx];
            if (row_idx * K + k > minP.eles.size() || col_idx * K + k > minQ.eles.size() )
            {
                printf("Psz %ld  idx %ld  Qsz %ld  idx %ld  real_hash_idx %ld row_idx %ld col_idx %ld Pblock_id %d QblockId %d Psta  %d Qsta %d\n", minP.eles.size(), row_idx * K + k ,   minQ.eles.size(), col_idx * K + k, real_hash_idx, row_idx, col_idx, minP.block_id, minQ.block_id, minP.sta_idx, minQ.sta_idx );
                getchar();
            }
            sum += minP.eles[row_idx * K + k] * minQ.eles[col_idx * K + k];
            //printf("k=%d  Pv %lf  Qv %lf  sum=%lf\n", k, minP.eles[row_idx * K + k],  minQ.eles[col_idx * K + k], sum);
        }
        /*
                if (sum > iter->second)
                {
                    positve_cnt++;
                }
                else
                {
                    negative_cnt++;
                    //printf("sum = %lf  real=%lf\n", sum, iter->second );
                }
                **/
        rmse += (sum - iter->second) * (sum - iter->second);
        cnt++;
    }
    //printf("RTestMap sz %ld cnt = %d\n", RTestMap.size(), cnt );
    rmse /= cnt;
    rmse = sqrt(rmse);
    //printf("positve_cnt=%d negative_cnt=%d\n", positve_cnt, negative_cnt );
    return rmse;
}
void  FilterDataSet(map<long, double>& RTestMap, long row_sta_idx, long row_len, long col_sta_idx, long col_len)
{
    //printf("Entering FilterDataSet\n");
    std::map<long, double>::iterator iter;
    long mem_cnt = 0;
    for (iter = TestMap.begin(); iter != TestMap.end(); iter++)
    {
        long hash_idx = iter->first;
        long r_idx = hash_idx / M;
        long c_idx = hash_idx % M;
        if (row_sta_idx <= r_idx && r_idx < row_sta_idx + row_len && col_sta_idx <= c_idx && c_idx < col_sta_idx + col_len)
        {
            RTestMap.insert(pair<long, double>(iter->first, iter->second));
        }

    }

    //printf("Entering Test FilterDataSet  %ld\n", RTestMap.size());


}
void CalcUpdt(int thread_id)
{
    while (1 == 1)
    {
        if (StartCalcUpdt[thread_id])
        {
            int times_thresh = 1000;
            int row_sta_idx = Pblock.sta_idx;
            int col_sta_idx = Qblock.sta_idx;
            size_t rtsz = hash_for_row_threads[thread_id].size();
            size_t ctsz = hash_for_col_threads[thread_id].size();
            int rand_idx = -1;
            while (times_thresh--)
            {
                rand_idx = random() % rtsz;
                long real_hash_idx = hash_for_row_threads[thread_id][rand_idx];
                long i = real_hash_idx / M - row_sta_idx;
                long j = real_hash_idx % M - col_sta_idx;
                double error = rates_for_row_threads[thread_id][rand_idx];
                for (int k = 0; k < K; ++k)
                {
                    error -= oldP[i * K + k] * oldQ[j * K + k];
                }
                for (int k = 0; k < K; ++k)
                {
                    Pupdt.eles[i * K + k] += yita * (error * oldQ[j * K + k] - theta * oldP[i * K + k]);
                }

                rand_idx = random() % ctsz;
                real_hash_idx = hash_for_col_threads[thread_id][rand_idx];
                i = real_hash_idx / M - row_sta_idx;
                j = real_hash_idx % M - col_sta_idx;
                error = rates_for_col_threads[thread_id][rand_idx];
                for (int k = 0; k < K; ++k)
                {
                    error -= oldP[i * K + k] * oldQ[j * K + k];
                }
                for (int k = 0; k < K; ++k)
                {
                    Qupdt.eles[j * K + k] += yita * (error * oldP[i * K + k] - theta * oldQ[j * K + k]);
                }
            }
            StartCalcUpdt[thread_id] = false;
            //printf("finish %d\n",  thread_id);

        }
    }


}
void submf()
{
    printf("enter submf\n");
    double error = 0;
    int minN = Pblock.height;
    int minM = Qblock.height;

    int row_sta_idx = Pblock.sta_idx;
    int col_sta_idx = Qblock.sta_idx;
    int row_len = Pblock.height;
    int col_len = Qblock.height;

    int Psz = Pblock.height * K;
    int Qsz = Qblock.height * K;
    //printf("Psz =%d Qsz =%d\n", Psz, Qsz);
    Pupdt.eles.resize(Psz);
    Pupdt.ele_num = Psz;
    Qupdt.eles.resize(Qsz);
    Qupdt.ele_num = Qsz;
    Pupdt.block_id = Pblock.block_id;
    Qupdt.block_id = Qblock.block_id;

    int r1 = Pblock.block_id * 2;
    int c1 = Qblock.block_id * 2;
    int f1 = r1 * 8 + c1;
    int f2 = r1 * 8 + c1 + 1;
    int f3 = (r1 + 1) * 8 + c1;
    int f4 = (r1 + 1) * 8 + c1 + 1;
    RMap.clear();
    LoadRmatrix(f1, RMap);
    LoadRmatrix(f2, RMap);
    LoadRmatrix(f3, RMap);
    LoadRmatrix(f4, RMap);


    int ii = 0;
    for (ii = 0; ii < Psz; ii++)
    {
        Pupdt.eles[ii] = 0;
    }
    for ( ii = 0; ii < Qsz; ii++)
    {
        Qupdt.eles[ii] = 0;
    }

    {
        int times_thresh = 1000;
        //for (int c_row_idx = 0; c_row_idx < row_len; c_row_idx++)
        //for (size_t ss = 0; ss < sample_sz; ss++)
        oldP = Pblock.eles;
        oldQ = Qblock.eles;

        hash_ids.clear();
        rates.clear();
        map<long, double>::iterator myiter = RMap.begin();
        long ridx = 0;
        long cidx = 0;
        for (int i = 0; i < WORKER_THREAD_NUM; i++)
        {
            hash_for_row_threads[i].clear();
            hash_for_col_threads[i].clear();
            rates_for_row_threads[i].clear();
            rates_for_col_threads[i].clear();
        }
        while (myiter != RMap.end())
        {
            //hash_ids.push_back(myiter->first);
            //rates.push_back(myiter->second);

            ridx = ((myiter->first) / M) % WORKER_THREAD_NUM;
            cidx = ((myiter->first) % M) % WORKER_THREAD_NUM;

            hash_for_row_threads[ridx].push_back(myiter->first);
            rates_for_row_threads[ridx].push_back(myiter->second);
            hash_for_col_threads[cidx].push_back(myiter->first);
            rates_for_col_threads[cidx].push_back(myiter->second);

            myiter++;
        }
        int tsz = hash_ids.size();
        int rand_idx = -1;
        //printf("launch new thread...\n");
        //multiple thread...
        //std::thread test(testhello, 10);
        //test.join();

        struct timeval beg, ed;
        memset(&beg, 0, sizeof(struct timeval));
        memset(&ed, 0, sizeof(struct timeval));
        gettimeofday(&beg, 0);
        //std::thread td(CalcUpdt, 0);
        //td.join();
        /*
                std::vector<thread> td_vec;
                for (int i = 0; i < WORKER_THREAD_NUM; i++)
                {
                    //std::thread td(CalcUpdt, i);
                    td_vec.push_back(std::thread(CalcUpdt, i));
                }
                //printf("come here\n");
                for (int i = 0; i < WORKER_THREAD_NUM; i++)
                {
                    td_vec[i].join();
                    printf("%d  has joined\n", i );
                }
        **/
        bool canbreak = true;
        for (int ii = 0; ii < WORKER_THREAD_NUM; ii++)
        {
            StartCalcUpdt[ii] = true;
        }

        while (1 == 1)
        {
            canbreak = true;
            for (int ii = 0; ii < WORKER_THREAD_NUM; ii++)
            {

                if (StartCalcUpdt[ii])
                {
                    //printf("ii=%d, %d \n", ii, StartCalcUpdt[ii] );
                    canbreak = false;
                }

            }
            if (canbreak)
            {
                break;
            }
            //printf("?break?  %d\n", canbreak);
            //std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }
        //printf("ccc\n");
        gettimeofday(&ed, 0);
        long long mksp = (ed.tv_sec - beg.tv_sec) * 1000000 + ed.tv_usec - beg.tv_usec;
        printf("thesh = %d  time = %lld\n", times_thresh, mksp);
        /*


        gettimeofday(&beg, 0);

        while (times_thresh--)
        {
            rand_idx = random() % tsz;
            long real_hash_idx = hash_ids[rand_idx];
            long i = real_hash_idx / M - row_sta_idx;
            long j = real_hash_idx % M - col_sta_idx;
            error = rates[rand_idx];
            for (int k = 0; k < K; ++k)
            {
                error -= oldP[i * K + k] * oldQ[j * K + k];
            }
            for (int k = 0; k < K; ++k)
            {
                Pupdt.eles[i * K + k] += yita * (error * oldQ[j * K + k] - theta * oldP[i * K + k]);
                Qupdt.eles[j * K + k] += yita * (error * oldP[i * K + k] - theta * oldQ[j * K + k]);
            }

        }
        gettimeofday(&ed, 0);
        long long mksp = (ed.tv_sec - beg.tv_sec) * 1000000 + ed.tv_usec - beg.tv_usec;
        printf("thesh = %d  time = %lld\n", times_thresh, mksp);

        **/
        /*
        int test_cnt = 0;
        for (int i = 0; i < Pupdt.eles.size(); i++)
        {
            if ( Pupdt.eles[i] != 0)
            {
                test_cnt++;
            }
        }
        gettimeofday(&ed, 0);
        mksp = (ed.tv_sec - beg.tv_sec) * 1000000 + ed.tv_usec - beg.tv_usec;
        printf("test_cnt=%d  time = %lld\n", test_cnt, mksp );
        **/
    }


}


int wait4connection(char*local_ip, int local_port)
{
    int fd = socket(PF_INET, SOCK_STREAM , 0);
    struct sockaddr_in address;
    bzero(&address, sizeof(address));
    //转换成网络地址
    address.sin_port = htons(local_port);
    address.sin_family = AF_INET;
    //地址转换
    inet_pton(AF_INET, local_ip, &address.sin_addr);
    //设置socket buffer大小
    //int recvbuf = 4096;
    //int len = sizeof( recvbuf );
    //setsockopt( fd, SOL_SOCKET, SO_RCVBUF, &recvbuf, sizeof( recvbuf ) );
    //getsockopt( fd, SOL_SOCKET, SO_RCVBUF, &recvbuf, ( socklen_t* )&len );
    //printf( "the receive buffer size after settting is %d\n", recvbuf );
    //绑定ip和端口
    int check_ret = -1;
    do
    {
        printf("binding...\n");
        check_ret = bind(fd, (struct sockaddr*)&address, sizeof(address));
        std::this_thread::sleep_for(std::chrono::milliseconds(1000));
    }
    while (check_ret >= 0);
    printf("bind ok\n");
    //创建监听队列，用来存放待处理的客户连接
    check_ret = listen(fd, 5);
    assert(check_ret >= 0);

    struct sockaddr_in addressClient;
    socklen_t clientLen = sizeof(addressClient);

    printf("thread %d listening at %s %d\n", thread_id, local_ip, local_port );
    //接受连接，阻塞函数
    int connfd = accept(fd, (struct sockaddr*)&addressClient, &clientLen);
    return connfd;

}
void sendTd(int send_thread_id)
{
    printf("send_thread_id=%d\n", send_thread_id);
    char* remote_ip = remote_ips[send_thread_id];
    int remote_port = remote_ports[send_thread_id];

    int fd;
    int check_ret;
    fd = socket(PF_INET, SOCK_STREAM , 0);
    assert(fd >= 0);

    struct sockaddr_in address;
    bzero(&address, sizeof(address));
    //转换成网络地址
    address.sin_port = htons(remote_port);
    address.sin_family = AF_INET;
    //地址转换
    inet_pton(AF_INET, remote_ip, &address.sin_addr);
    do
    {
        check_ret = connect(fd, (struct sockaddr*) &address, sizeof(address));
        std::this_thread::sleep_for(std::chrono::milliseconds(1000));
    }
    while (check_ret < 0);
    assert(check_ret >= 0);
    //发送数据
    printf("connect to %s %d\n", remote_ip, remote_port);
    while (1 == 1)
    {
        /*
        if (!canSend)
        {
            //printf("Td %d cannotSend...\n", thread_id );
            std::this_thread::sleep_for(std::chrono::milliseconds(1000));
        }
        else
        **/
        if (canSend)
        {
            //printf("Td:%d cansend\n", thread_id );
            size_t struct_sz = sizeof(Pupdt);
            size_t data_sz = sizeof(double) * Pupdt.eles.size();
            char* buf = (char*)malloc(struct_sz + data_sz);
            memcpy(buf, &(Pupdt), struct_sz);
            memcpy(buf + struct_sz, (char*) & (Pupdt.eles[0]), data_sz);
            /*
            int ret = send(fd, buf, (struct_sz + data_sz), 0);
            if (ret >= 0 )
            {
                //printf("[Id:%d] send success \n", thread_id);
            }
            **/
            size_t total_len = struct_sz + data_sz;
            struct timeval st, et, tspan;
            size_t sent_len = 0;
            size_t remain_len = total_len;
            int ret = -1;
            size_t to_send_len = 4096;
            gettimeofday(&st, 0);


            while (remain_len > 0)
            {
                if (to_send_len > remain_len)
                {
                    to_send_len = remain_len;
                }
                //printf("sending...\n");
                ret = send(fd, buf + sent_len, to_send_len, 0);
                if (ret >= 0)
                {
                    remain_len -= to_send_len;
                    sent_len += to_send_len;
                    //printf("remain_len = %ld\n", remain_len);
                }
                else
                {
                    printf("still fail\n");
                }
                //getchar();
            }
            free(buf);


            struct_sz = sizeof(Qupdt);
            data_sz = sizeof(double) * Qupdt.eles.size();
            total_len = struct_sz + data_sz;
            buf = (char*)malloc(struct_sz + data_sz);
            memcpy(buf, &(Qupdt), struct_sz);
            memcpy(buf + struct_sz , (char*) & (Qupdt.eles[0]), data_sz);
            /*
            ret = send(fd, buf, (struct_sz + data_sz), 0);
            if (ret >= 0 )
            {
                //printf("[Id:%d] send success \n", thread_id);
            }
            **/

            sent_len = 0;
            remain_len = total_len;
            ret = -1;
            to_send_len = 4096;
            while (remain_len > 0)
            {
                if (to_send_len > remain_len)
                {
                    to_send_len = remain_len;
                }
                //printf("sending...\n");
                ret = send(fd, buf + sent_len, to_send_len, 0);
                if (ret >= 0)
                {
                    remain_len -= to_send_len;
                    sent_len += to_send_len;
                    //printf("remain_len = %ld\n", remain_len);
                }
                else
                {
                    printf("still fail\n");
                }
                //getchar();
            }


            free(buf);
            gettimeofday(&et, 0);
            long long mksp = (et.tv_sec - st.tv_sec) * 1000000 + et.tv_usec - st.tv_usec;
            printf("send two blocks mksp=%lld\n", mksp );
            canSend = false;
        }
    }

}
void recvTd(int recv_thread_id)
{
    printf("recv_thread_id=%d\n", recv_thread_id);
    int connfd = wait4connection(local_ips[recv_thread_id], local_ports[recv_thread_id] );

    printf("[Td:%d] worker get connection\n", recv_thread_id);
    while (1 == 1)
    {
        //printf("check 0\n");
        size_t expected_len = sizeof(Pblock);
        char* sockBuf = (char*)malloc(expected_len + 100);
        size_t cur_len = 0;
        int ret = 0;
        //printf("check 1  expected_len=%ld sockBuf=%p\n", expected_len, sockBuf);

        while (cur_len < expected_len)
        {
            //printf("cur_len = %ld  expected_len = %ld\n", cur_len, expected_len);
            ret = recv(connfd, sockBuf + cur_len, expected_len - cur_len, 0);
            //printf("check 1.5\n");
            if (ret < 0)
            {
                printf("Mimatch!\n");
            }
            cur_len += ret;
        }


        struct Block* pb = (struct Block*)(void*)sockBuf;
        Pblock.block_id = pb->block_id;
        Pblock.data_age = pb->data_age;
        Pblock.sta_idx = pb->sta_idx;
        Pblock.height = pb->height;
        Pblock.ele_num = pb->ele_num;
        Pblock.eles.resize(pb->ele_num);


        size_t data_sz = sizeof(double) * (Pblock.ele_num);
        sockBuf = (char*)malloc(data_sz);
        //printf("check 4\n");
        cur_len = 0;
        ret = 0;
        while (cur_len < data_sz)
        {
            ret = recv(connfd, sockBuf + cur_len, data_sz - cur_len, 0);
            if (ret < 0)
            {
                printf("Mimatch!\n");
            }
            cur_len += ret;
        }
        //printf("check 5\n");
        double* data_eles = (double*)(void*)sockBuf;
        for (int i = 0; i < Pblock.ele_num; i++)
        {
            Pblock.eles[i] = data_eles[i];
        }
        free(data_eles);


        expected_len = sizeof(Pblock);
        sockBuf = (char*)malloc(expected_len);
        cur_len = 0;
        ret = 0;
        while (cur_len < expected_len)
        {
            ret = recv(connfd, sockBuf + cur_len, expected_len - cur_len, 0);
            if (ret < 0)
            {
                printf("Mimatch!\n");
            }
            cur_len += ret;
        }
        struct Block* qb = (struct Block*)(void*)sockBuf;
        Qblock.block_id = qb->block_id;
        Qblock.data_age = qb->data_age;
        Qblock.sta_idx = qb->sta_idx;
        Qblock.height = qb->height;
        Qblock.ele_num = qb-> ele_num;
        Qblock.eles.resize(qb->ele_num);
        free(sockBuf);

        data_sz = sizeof(double) * (Qblock.ele_num);
        sockBuf = (char*)malloc(data_sz);

        cur_len = 0;
        ret = 0;
        while (cur_len < data_sz)
        {
            ret = recv(connfd, sockBuf + cur_len, data_sz - cur_len, 0);
            if (ret < 0)
            {
                printf("Mimatch!\n");
            }
            cur_len += ret;
        }

        data_eles = (double*)(void*)sockBuf;
        for (int i = 0; i < Qblock.ele_num; i++)
        {
            Qblock.eles[i] = data_eles[i];
        }
        free(data_eles);

        //printf("[ID:%d] get Qblock %d\n", thread_id, Qblock.block_id);
        //Qblock.printBlock();


        hasRecved = true;
    }
}


