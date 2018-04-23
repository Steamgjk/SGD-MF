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
#define FILE_NAME "./netflix_row.txt"

#define WORKER_NUM 1
char* remote_ips[10] = {"12.12.10.18", "12.12.10.18", "12.12.10.18", "12.12.10.18"};
int remote_ports[10] = {4411, 4412, 4413, 4414};

char* local_ips[10] = {"12.12.10.12", "12.12.10.15", "12.12.10.16", "12.12.10.17"};
int local_ports[10] = {5511, 5512, 5513, 5514};

#define N  17770 // row number
#define M  2649429 //col number
#define K  40 //主题个数

#define Bsz (100*1000)
#define Rsz (17770 * Bsz)

#define ThreshIter 100
#define SEQ_LEN 5000

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

bool canSend = false;
bool hasRecved = false;

int block_seq[SEQ_LEN];

int wait4connection(char*local_ip, int local_port);
void sendTd(int send_thread_id);
void recvTd(int recv_thread_id);
//void submf(double *minR, Block& minP, Block& minQ, Updates& updateP, Updates& updateQ,  int minK, int steps = 50, float alpha = 0.0002, float beta = 0.02);
void submf(Block& minP, Block& minQ, Updates& updateP, Updates& updateQ,  int minK, int steps = 50, float alpha = 0.0002, float beta = 0.02);

void LoadConfig(char*filename);
void WriteLog(Block&Pb, Block&Qb);
void getMinR(double* minR, int row_sta_idx, int row_len, int col_sta_idx, int col_len);
void LoadRating();
int thread_id = -1;

struct timeval start, stop, diff;


//double* minR = (double*)malloc(sizeof(double) * Rsz);
map<long, double> RMap;
vector<long> KeyVec;
int main(int argc, const char * argv[])
{

    //printf("minR = %p sz = %lld\n", minR, (sizeof(double) * Rsz / 1000000000) );
    //getchar();
    /*
        for (int i = 0; i < Rsz; i++)
    {
        minR[i] = 0;
    }

    **/
    thread_id = atoi(argv[1]);
    LoadRating();
    printf("Load Rating Success\n");


    memset(&start, 0, sizeof(struct timeval));
    memset(&stop, 0, sizeof(struct timeval));
    memset(&diff, 0, sizeof(struct timeval));

    std::thread send_thread(sendTd, thread_id);
    send_thread.detach();

    std::thread recv_thread(recvTd, thread_id);
    recv_thread.detach();
    //double* minR = (double*)malloc(sizeof(double) * 1000);
    int iter_cnt = 0;
    bool isstart = false;
    while (1 == 1)
    {

        if (hasRecved)
        {
            printf("has Received\n");
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
            //printf("ele_num = %d   size = %ld\n", ele_num, sizeof(double) * ele_num);
            //getchar();
            //double* minR = (double*)malloc(sizeof(double) * col_len);
            //double* minR = (double*)malloc(sizeof(double) * col_len);
            //printf("minR=%p sizeof(double) * M=%lld\n", minR, M );
            //getchar();


            //printf("okkkk minR=%p\n", minR);
            //getMinR(minR, row_sta_idx, row_len, col_sta_idx, col_len);
            /*
            for (int i = 0; i < ele_num; i++)
            {
                printf("%lf\t", minR[i] );
            }
            **/
            //printf("fin  before minR  %p\n", minR);
            submf(Pblock, Qblock, Pupdt, Qupdt, K);
            iter_cnt++;
            if (iter_cnt == ThreshIter)
            {
                gettimeofday(&stop, 0);

                long long mksp = (stop.tv_sec - start.tv_sec) * 1000000 + stop.tv_usec - start.tv_usec;
                printf("itercnt = %d  time = %lld\n", iter_cnt, mksp);
                WriteLog(Pblock, Qblock);
                exit(0);
            }

            //free(minR);

            canSend = true;
            hasRecved = false;

        }
        else
        {
            //printf("[Id:%d] has not received...\n", thread_id );
            std::this_thread::sleep_for(std::chrono::milliseconds(1000));
        }
    }
    /** Test
    int row_sta_idx = 13;
    int row_len = 3;
    int col_sta_idx = 1;
    int col_len = 4;
    int ele_num = row_len * col_len;
    double* minR = (double*)malloc(sizeof(double) * ele_num);
    getMinR(minR, row_sta_idx, row_len, col_sta_idx, col_len);
    for (int i = 0 ; i < row_len; i++)
    {
    for (int j = 0; j < col_len; j++)
    {
    printf("%lf ", minR[i * col_len + j] );
    }
    printf("\n");
    }
    return 0;
    **/
}

void LoadConfig(char*filename)
{

}
void LoadRating()
{
    ifstream ifs(FILE_NAME);
    if (!ifs.is_open())
    {
        printf("fail to open the file %s\n", FILE_NAME);
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
        //KeyVec.insert(hash_idx);
        cnt++;
        if (cnt % 1000000 == 0)
        {
            printf("cnt=%d\n", cnt );
        }
    }

    printf("cnt=%d sizeof(long)=%ld\n", cnt, sizeof(long));
}
void WriteLog(Block&Pb, Block&Qb)
{
    char fn[100];
    sprintf(fn, "Pblock-%d", Pb.block_id);
    ofstream pofs(fn, ios::trunc);
    for (int h = 0; h < Pb.height; h++)
    {
        for (int j = 0; j < K; j++)
        {
            pofs << Pb.eles[h * K + j] << " ";
        }
        pofs << endl;
    }
    sprintf(fn, "Qblock-%d", Qb.block_id);
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
void getMinR(double* minR, int row_sta_idx, int row_len, int col_sta_idx, int col_len)
{
    //printf("row_sta_idx = %d row_len=%d col_sta_idx=%d  col_len = %d\n", row_sta_idx, row_len, col_sta_idx, col_len);


    ifstream ifs(FILE_NAME);
    if (!ifs.is_open())
    {
        printf("fail to open the file %s\n", FILE_NAME);
        exit(-1);
    }
    string temp;
    for (int i = 0; i < row_sta_idx; i++)
    {
        getline(ifs, temp);
        //cout << "temp:\t" << temp << endl;
    }
    //printf("check cc 1\n");
    int line_no = row_sta_idx;
    double temp_db;
    int total_num = row_len * col_len;
    int cnt = 0;
    //printf("check cc 2\n");

    for (int i = row_sta_idx; i < row_sta_idx + row_len; i++)
    {
        if (i % 100 == 0)
        {
            printf("getMinR i = %d\n", i );
        }
        for (int j = 0 ; j < col_sta_idx; j++)
        {
            ifs >> temp_db;
            //cout << "tf " << temp_db << endl;
        }
        //cout << endl;
        for (int j = col_sta_idx; j < col_sta_idx + col_len; j++)
        {
            //printf("j=%d cnt=%d minR=%p\n", j, cnt, minR);
            //ifs >> temp_db;
            //minR[cnt] = temp_db;
            //printf("temp_db=%lf\n", temp_db );
            ifs >> minR[cnt];
            //cout << "minR " << minR[cnt] << endl;
            cnt++;
        }
        //cout << endl;
        //getchar();
        //printf("com here\n");
        //getchar();
        for (int j = col_sta_idx + col_len; j < M; j++)
        {
            ifs >> temp_db;
            //cout << "tfb " << temp_db << endl;
            //getchar();
        }
        //getchar();
    }
    //printf("Returned  \n");
}

void submf(Block & minP, Block & minQ, Updates & updateP, Updates & updateQ, int minK, int steps, float alpha , float beta)
{
    printf("begin submf\n");
    double error = 0;
    int minN = minP.height;
    int minM = minQ.height;

    int row_sta_idx = minP.sta_idx;
    int col_sta_idx = minQ.sta_idx;

    int Psz =  minP.height * minK;
    int Qsz = minQ.height * minK;
    //printf("Psz =%d Qsz =%d\n", Psz, Qsz);
    updateP.eles.resize(Psz);
    updateP.ele_num = Psz;
    updateQ.eles.resize(Qsz);
    updateQ.ele_num = Qsz;
    int ii = 0;
    for (ii = 0; ii < Psz; ii++)
    {
        updateP.eles[ii] = 0;
    }
    for ( ii = 0; ii < Qsz; ii++)
    {
        updateQ.eles[ii] = 0;
    }
    vector<double> originalP = minP.eles;
    vector<double> originalQ = minQ.eles;
    /*
        vector<int> vshuf(minM * minN);
        for (int i = 0; i < minM * minN; i++)
        {
            vshuf[i] = i;
        }
        random_shuffle(vshuf.begin(), vshuf.end());//迭代器

    **/
    map<long, double>::iterator iter;
    KeyVec.clear();
    for (iter = RMap.begin(); iter != RMap.end(); iter++)
    {
        int real_hash_idx = iter->first;
        int real_row_idx = real_hash_idx / M;
        int real_col_idx = real_hash_idx % M;
        if (row_sta_idx <= real_row_idx && real_row_idx < row_sta_idx + row_len \
                && col_sta_idx <= real_col_idx && real_col_idx < col_sta_idx + col_len )
        {
            KeyVec.push_back(real_hash_idx);
        }
        if (KeyVec.size() % 1000000 == 0)
        {
            printf("sz = %ld\n", KeyVec.size() );
        }
    }
    /*
    for (int i = 0; i < minN; ++i)
    {
        for (int j = 0; j < minM; ++j)
        {
            long real_row_idx = i + row_sta_idx;
            long real_col_idx = j + col_sta_idx;
            long real_hash_idx = real_row_idx * M + real_col_idx;
            iter = RMap.find(real_hash_idx);
            if (iter != RMap.end())
            {
                KeyVec.push_back(real_hash_idx);
            }
        }
    }
    **/
    int tm = rand() % 10;
    for (int i = 0 ; i < tm; i++)
    {
        random_shuffle(KeyVec.begin(), KeyVec.end());//迭代器
    }

    printf("Begin Calc sz = %ld\n", KeyVec.size() );
    //for (int step = 0; step < steps; ++step)

    {
        //for (int i = 0; i < minN; ++i)
        {
            //for (int j = 0; j < minM; ++j)
            for (int ii = 0; ii < KeyVec.size(); ii++)
            {
                if (ii % 1000000 == 0)
                {
                    printf("ii=%d\n", ii);
                }
                int real_hash_idx = KeyVec[ii];
                long real_row_idx = real_hash_idx / M;
                long real_col_idx = real_hash_idx % M;
                int i = real_row_idx - row_sta_idx;
                int j = real_col_idx - col_sta_idx;
                map<long, double>::iterator iter;
                iter = RMap.find(real_hash_idx);
                if (iter != RMap.end())
                {

                    //printf("idx = %d\n", i * minM + j );
                    //这里面的error 就是公式6里面的e(i,j)
                    //error = minR[i * minM + j];
                    //error = minR[j];
                    error = iter->second;
                    //printf("error = %lf\n", error );
                    for (int k = 0; k < minK; ++k)
                    {
                        //error_m -= P[i * minK + k] * Q[k * minM + j];
                        error -= minP.eles[i * minK + k] * minQ.eles[j * minK + k];
                    }

                    //更新公式6
                    for (int k = 0; k < minK; ++k)
                    {
                        //printf("minP sz = %ld minQ sz =%ld updt sz %ld updt sz %ld\n i*minK+k=%d  j*minK+k=%d\n", minP.eles.size(), minQ.eles.size(), updateP.eles.size(), updateQ.eles.size(), i * minK + k, j * minK + k );
                        //updateP.eles[i * minK + k] += alpha * (2 * error * minQ.eles[j * minK + k] - beta * minP.eles[i * minK + k]);
                        //updateQ.eles[j * minK + k] += alpha * (2 * error * minP.eles[i * minK + k] - beta * minQ.eles[j * minK + k]);
                        minP.eles[i * minK + k] += alpha * (2 * error * minQ.eles[j * minK + k] - beta * minP.eles[i * minK + k]);
                        minQ.eles[j * minK + k] += alpha * (2 * error * minP.eles[i * minK + k] - beta * minQ.eles[j * minK + k]);
                    }


                }
            }
            for (int i = 0; i < originalP.size(); i++)
            {
                updateP.eles[i] = minP.eles[i] - originalP[i];
            }
            for (int j = 0; j < originalQ.size(); j++)
            {
                updateQ.eles[j] = minQ.eles[j] - originalQ[j];
            }

        }

        printf("end sumbmf\n");
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
    int sendbuf = 4096;
    int len = sizeof( sendbuf );
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
        if (!canSend)
        {
            //printf("Td %d cannotSend...\n", thread_id );
            std::this_thread::sleep_for(std::chrono::milliseconds(1000));
        }
        else
        {
            //printf("Td:%d cansend\n", thread_id );
            size_t struct_sz = sizeof(Pupdt);
            size_t data_sz = sizeof(double) * Pupdt.eles.size();
            char* buf = (char*)malloc(struct_sz + data_sz);
            memcpy(buf, &(Pupdt), struct_sz);
            memcpy(buf + struct_sz, (char*) & (Pupdt.eles[0]), data_sz);
            int ret = send(fd, buf, (struct_sz + data_sz), 0);
            if (ret >= 0 )
            {
                printf("[Id:%d] send success \n", thread_id);
            }
            free(buf);

            struct_sz = sizeof(Qupdt);
            data_sz = sizeof(double) * Qupdt.eles.size();
            buf = (char*)malloc(struct_sz + data_sz);
            memcpy(buf, &(Qupdt), struct_sz);
            memcpy(buf + struct_sz , (char*) & (Qupdt.eles[0]), data_sz);
            ret = send(fd, buf, (struct_sz + data_sz), 0);
            if (ret >= 0 )
            {
                printf("[Id:%d] send success \n", thread_id);
            }
            free(buf);
            //printf("Here we pause...\n");
            //getchar();
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

        //ret = recv(connfd, sockBuf, expected_len, 0);
        //printf("check 2\n");
        struct Block* pb = (struct Block*)(void*)sockBuf;
        Pblock.block_id = pb->block_id;
        Pblock.data_age = pb->data_age;
        Pblock.sta_idx = pb->sta_idx;
        Pblock.height = pb->height;
        Pblock.ele_num = pb->ele_num;
        Pblock.eles.resize(pb->ele_num);
        //printf("check 3\n");
        //free(sockBuf);

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
        //printf("[ID:%d] get Pblock %d\n", thread_id, Pblock.block_id);
        //Pblock.printBlock();

        //printf("Here recv pause...\n");
        //getchar();

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


