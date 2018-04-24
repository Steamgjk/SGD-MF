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
#include <atomic>
#include <fstream>
#include <map>
using namespace std;
#define CAP 30
#define FILE_NAME "./netflix_row.txt"
#define TEST_NAME "./test_out.txt"
int WORKER_NUM = 1;
char* local_ips[CAP] = {"12.12.10.18", "12.12.10.18", "12.12.10.18", "12.12.10.18"};
int local_ports[CAP] = {4411, 4412, 4413, 4414};
char* remote_ips[CAP] = {"12.12.10.12", "12.12.10.15", "12.12.10.16", "12.12.10.17"};
int remote_ports[CAP] = {5511, 5512, 5513, 5514};

#define N  17770 // row number
#define M  2649429 //col number
#define K  40 //主题个数

//double R[N][M];
//double Rline[M];
map<long, double> RMap;
map<long, double> TestMap;
double P[N][K];
double Q[K][M];
bool worker_debug = false;
bool main_debug = false;

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
struct Block Pblocks[CAP];
struct Block Qblocks[CAP];
struct Updates Pupdts[CAP];
struct Updates Qupdts[CAP];




int wait4connection(char*local_ip, int local_port);
void sendTd(int send_thread_id);
void recvTd(int recv_thread_id);
void printBlockPair(Block& pb, Block& qb, int minK);
double CalcRMSE();
void partitionP(int portion_num,  Block* Pblocks);
void partitionQ(int portion_num,  Block* Qblocks);
void getMinR(double* minR, int row_sta_idx, int row_len, int col_sta_idx, int col_len);
void LoadRating();
void LoadTestRating();
atomic_int recvCount(0);
bool canSend[CAP] = {false};
int worker_pidx[CAP];
int worker_qidx[CAP];

int main(int argc, const char * argv[])
{
    //int connfd = wait4connection(ips[0], ports[0]);
    //printf("connfd=%d\n", connfd);
    //gen P and Q
    if (argc == 2)
    {
        WORKER_NUM = atoi(argv[1]) ;
    }
    srand(1);
    ofstream log_ofs("./rmse.txt", ios::trunc);
    /*
    for (int i = 0; i < N; i++)
    {
        getMinR(R[i], i, 1, 0, M);
        printf("Load %d line\n", i);
    }
    for (int i = 0 ; i < N; i++)
    {
        for (int j = 0; j < M; j++)
        {
            printf("%lf\t", R[i][j] );
        }
        printf("\n");
    }
    **/
    LoadRating();
    LoadTestRating();
    printf("Load Complete\n");
    for (int i = 0; i < N; i++)
    {
        for (int j = 0; j < K; j++)
        {
            //P[i][j] =  ((double)rand() / RAND_MAX ) / sqrt(K);
            //P[i][j] = drand48() / 2;
            P[i][j] =  ((double)(rand() % 100) ) / 120 ;
        }
    }
    for (int i = 0; i < K; i++)
    {
        for (int j = 0; j < M; j++)
        {
            //Q[i][j] = ((double)rand() / RAND_MAX) / sqrt(K);
            //Q[i][j] = drand48() / 2;
            Q[i][j] =  ((double)(rand() % 100) ) / 120 ;

        }
    }
    for (int i = 0; i < WORKER_NUM; i++)
    {
        canSend[i] = false;
    }

    for (int i = 0; i < WORKER_NUM; i++)
    {
        worker_pidx[i] = worker_qidx[i] = i;
    }
    for (int send_thread_id = 0; send_thread_id < WORKER_NUM; send_thread_id++)
    {
        std::thread send_thread(sendTd, send_thread_id);
        send_thread.detach();
    }
    for (int recv_thread_id = 0; recv_thread_id < WORKER_NUM; recv_thread_id++)
    {
        std::thread recv_thread(recvTd, recv_thread_id);
        recv_thread.detach();
    }
    int iter_t = 0;

    while (1 == 1)
    {
        partitionP(WORKER_NUM, Pblocks);
        partitionQ(WORKER_NUM, Qblocks);
        for (int i = 0; i < WORKER_NUM; i++)
        {
            // Pblocks[i].printBlock();
            //Qblocks[i].printBlock();
        }
        //getchar();
        srand(time(0));
        bool ret = false;

        int per_num = 1;
        for (int i = 0; i < WORKER_NUM; i++)
        {
            worker_pidx[i] = worker_qidx[i] = i;
            per_num = per_num * (i + 1);
        }
        /*
        int rand_round = rand() % per_num;
        for (int i = 0; i < rand_round; i++)
        {
            next_permutation(worker_pidx, worker_pidx + WORKER_NUM);
        }
        rand_round = rand() % per_num;
        for (int i = 0; i < rand_round; i++)
        {
            next_permutation(worker_qidx, worker_qidx + WORKER_NUM);
        }
        **/
        random_shuffle(worker_pidx, worker_pidx + WORKER_NUM); //迭代器
        random_shuffle(worker_qidx, worker_qidx + WORKER_NUM); //迭代器


        for (int i = 0; i < WORKER_NUM; i++)
        {
            printf("%d:[%d:%d]\n", i, worker_pidx[i], worker_qidx[i] );
        }
        for (int i = 0; i < WORKER_NUM; i++)
        {
            canSend[i] = true;
        }

        while (recvCount != WORKER_NUM)
        {
            //cout << "RecvCount\t" << recvCount << endl;
            //std::this_thread::sleep_for(std::chrono::milliseconds(1000));
        }
        if (recvCount == WORKER_NUM)
        {
            printf("Collect All, Can Update\n");
            int idx = 0;
            for (int kk = 0; kk < WORKER_NUM; kk++)
            {
                //Update P [N*K]
                for (int ii = 0; ii < Pupdts[kk].eles.size(); ii++)
                {
                    int row_idx = (ii + idx) / K;
                    int col_idx = (ii + idx) % K;
                    P[row_idx][col_idx] += Pupdts[kk].eles[ii];
                }
            }
            for (int kk = 0; kk < WORKER_NUM; kk++)
            {
                //Update Q[K*M]
                for (int ii = 0; ii < Qupdts[kk].eles.size(); ii++)
                {
                    int col_idx = (ii + idx) / K;
                    int row_idx = (ii + idx) % K;
                    Q[row_idx][col_idx] += Qupdts[kk].eles[ii];
                }
            }
            printf("Update Finish, Can Distribute\n");
            //if (iter_t % 10 == 0)
            {
                double rmse = CalcRMSE();
                printf("rmse=%lf\n", rmse);
                log_ofs << rmse << endl;

            }
            recvCount = 0;
        }
        iter_t++;
    }

    return 0;
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
        cnt++;
        if (cnt % 1000000 == 0)
        {
            printf("cnt = %ld\n", cnt );
        }
    }

    printf("cnt=%d sizeof(long)=%ld\n", cnt, sizeof(long));
}
void LoadTestRating()
{
    ifstream ifs(TEST_NAME);
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
void sendTd(int send_thread_id)
{
    printf("send_thread_id=%d\n", send_thread_id);
    char* remote_ip = remote_ips[send_thread_id];
    int remote_port = remote_ports[send_thread_id];

    int fd;
    int check_ret;
    fd = socket(PF_INET, SOCK_STREAM , 0);
    //printf("fd = %d\n", fd);
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
        printf("[Td:%d] trying to connect %s %d\n", send_thread_id, remote_ip, remote_port );
        std::this_thread::sleep_for(std::chrono::milliseconds(1000));
    }
    while (check_ret < 0);
    assert(check_ret >= 0);
    printf("[Td:%d]connected %s  %d\n", send_thread_id, remote_ip, remote_port );
    while (1 == 1)
    {
        if (!canSend[send_thread_id])
        {
            std::this_thread::sleep_for(std::chrono::milliseconds(1000));
        }
        else
        {
            int pbid = worker_pidx[send_thread_id];
            int qbid = worker_qidx[send_thread_id];
            size_t struct_sz = sizeof( Pblocks[pbid]);
            size_t data_sz = sizeof(double) * Pblocks[pbid].eles.size();
            char* buf = (char*)malloc(struct_sz + data_sz);
            memcpy(buf, &(Pblocks[pbid]), struct_sz);
            memcpy(buf + struct_sz, (char*) & (Pblocks[pbid].eles[0]), data_sz);
            int ret = send(fd, buf, (struct_sz + data_sz), 0);
            if (ret >= 0 )
            {
                printf("[Td:%d] send success pbid =%d\n", send_thread_id, pbid );
            }
            free(buf);

            struct_sz = sizeof( Qblocks[qbid]);
            data_sz = sizeof(double) * Qblocks[qbid].eles.size();
            buf = (char*)malloc(struct_sz + data_sz);
            memcpy(buf, &(Qblocks[qbid]), struct_sz);
            memcpy(buf + struct_sz , (char*) & (Qblocks[qbid].eles[0]), data_sz);
            ret = send(fd, buf, (struct_sz + data_sz), 0);
            if (ret >= 0 )
            {
                printf("[Td:%d] send success qbid=%d\n", send_thread_id, qbid);
            }
            free(buf);
            canSend[send_thread_id] = false;
        }
    }

}

void recvTd(int recv_thread_id)
{
    printf("recv_thread_id=%d\n", recv_thread_id);
    int connfd = wait4connection(local_ips[recv_thread_id], local_ports[recv_thread_id] );
    while (1 == 1)
    {
        //printf("Loop recving...\n");
        size_t expected_len = sizeof(Updates);
        char* sockBuf = (char*)malloc(expected_len);
        size_t cur_len = 0;
        int ret = 0;
        while (cur_len < expected_len)
        {
            //printf("[Td:%d] cur_len = %ld expected_len-cur_len = %ld\n", recv_thread_id, cur_len, expected_len - cur_len );
            ret = recv(connfd, sockBuf + cur_len, expected_len - cur_len, 0);
            if (ret <=  0)
            {
                printf("Mimatch! %d\n", ret);
                if (ret == 0)
                {
                    exit(-1);
                }
            }
            cur_len += ret;
        }
        struct Updates* updt = (struct Updates*)(void*)sockBuf;
        int block_id = updt->block_id;
        Pupdts[block_id].block_id = block_id;
        Pupdts[block_id].clock_t = updt->clock_t;
        Pupdts[block_id].ele_num = updt->ele_num;
        Pupdts[block_id].eles.resize(updt->ele_num);
        free(sockBuf);

        size_t data_sz = sizeof(double) * (Pupdts[block_id].ele_num);
        sockBuf = (char*)malloc(data_sz);

        cur_len = 0;
        ret = 0;
        while (cur_len < data_sz)
        {
            ret = recv(connfd, sockBuf + cur_len, data_sz - cur_len, 0);
            if (ret <=  0)
            {
                printf("Mimatch! %d\n", ret);
                if (ret == 0)
                {
                    exit(-1);
                }
            }
            cur_len += ret;
        }

        double* data_eles = (double*)(void*)sockBuf;
        for (int i = 0; i < Pupdts[block_id].ele_num; i++)
        {
            Pupdts[block_id].eles[i] = data_eles[i];
        }
        free(data_eles);

        //printf("getPupdates block_id = %ld\n", block_id);
        //Pupdts[block_id].printUpdates();

        expected_len = sizeof(Updates);
        sockBuf = (char*)malloc(expected_len);
        cur_len = 0;
        ret = 0;
        while (cur_len < expected_len)
        {
            ret = recv(connfd, sockBuf + cur_len, expected_len - cur_len, 0);
            if (ret <=  0)
            {
                printf("Mimatch! %d\n", ret);
                if (ret == 0)
                {
                    exit(-1);
                }
            }
            cur_len += ret;
        }
        updt = (struct Updates*)(void*)sockBuf;
        block_id = updt->block_id;
        Qupdts[block_id].block_id = block_id;
        Qupdts[block_id].clock_t = updt->clock_t;
        Qupdts[block_id].ele_num = updt->ele_num;
        Qupdts[block_id].eles.resize(Qupdts[block_id].ele_num);
        free(sockBuf);

        data_sz = sizeof(double) * (updt->ele_num);
        sockBuf = (char*)malloc(data_sz);

        cur_len = 0;
        ret = 0;
        while (cur_len < data_sz)
        {
            ret = recv(connfd, sockBuf + cur_len, data_sz - cur_len, 0);
            if (ret <=  0)
            {
                printf("Mimatch! %d\n", ret);
                if (ret == 0)
                {
                    exit(-1);
                }
            }
            cur_len += ret;
        }

        data_eles = (double*)(void*)sockBuf;
        for (int i = 0; i < Qupdts[block_id].ele_num; i++)
        {
            Qupdts[block_id].eles[i] = data_eles[i];
        }
        free(data_eles);
        //printf("get Qupdts\n");
        //Qupdts[block_id].printUpdates();
        recvCount++;
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
        printf("binding... %s  %d\n", local_ip, local_port);
        check_ret = bind(fd, (struct sockaddr*)&address, sizeof(address));
        std::this_thread::sleep_for(std::chrono::milliseconds(1000));
    }
    while (check_ret >= 0);

    //创建监听队列，用来存放待处理的客户连接
    check_ret = listen(fd, 5);
    assert(check_ret >= 0);
    printf("listening... %s  %d\n", local_ip, local_port);
    struct sockaddr_in addressClient;
    socklen_t clientLen = sizeof(addressClient);
    //接受连接，阻塞函数
    int connfd = accept(fd, (struct sockaddr*)&addressClient, &clientLen);
    printf("get connection from %s  %d\n", inet_ntoa(addressClient.sin_addr), addressClient.sin_port);
    return connfd;

}




void printBlockPair(Block& pb, Block& qb, int minK)
{
    /*
    double rmse = 0.0;
    printf("\n********Below P[%d]*************\n", pb.block_id);
    for (int i = 0 ; i < pb.height; i++)
    {
        for (int j = 0; j < minK; j++)
        {
            printf("%lf\t", pb.eles[i * minK + j]);
        }
        printf("\n");
    }
    printf("\n+++++++++++Below Q[%d]+++++++++++\n", qb.block_id);
    for (int i = 0 ; i < qb.height; i++)
    {
        for (int j = 0; j < minK; j++)
        {
            printf("%lf\t", qb.eles[i * minK + j]);
        }
        printf("\n");
    }
    printf("\n++++++++Below Rb++++++++++\n");

    for (int i = 0; i < pb.height; i++)
    {
        for (int j = 0; j < qb.height; j++)
        {
            double e = 0;
            for (int k = 0; k < minK; k++)
            {
                e += pb.eles[i * minK + k] * qb.eles[j * minK + k];
            }
            printf("%lf\t", e);
        }
        printf("\n");
    }
    printf("\n++++++++Below R+++++++++++++\n");

    int row_idx = pb.sta_idx;
    int col_idx = qb.sta_idx;

    for (int i = 0; i < pb.height; i++)
    {
        for (int j = 0; j < qb.height; j++)
        {
            double e = 0;
            for (int k = 0; k < minK; k++)
            {
                e += pb.eles[i * minK + k] * qb.eles[j * minK + k];
            }
            int row_idx = i + pb.sta_idx;
            int col_idx = j + qb.sta_idx;
            printf("%lf\t", R[row_idx][col_idx]);
            rmse += (e - R[row_idx][col_idx]) *  (e - R[row_idx][col_idx]);
        }
        printf("\n");
    }
    printf("\n***************************\n");
    printf("rmse=%lf\n", rmse);
    **/

}


void getMinR(double* minR, int row_sta_idx, int row_len, int col_sta_idx, int col_len)
{
    //printf("row_sta_idx = %d row_len=%d col_sta_idx=%d  col_len = %d\n", row_sta_idx, row_len, col_sta_idx, col_len);

    ifstream ifs(FILE_NAME);
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
        for (int j = 0 ; j < col_sta_idx; j++)
        {
            ifs >> temp_db;
            //cout << "tf " << temp_db << endl;
        }
        //cout << endl;
        for (int j = col_sta_idx; j < col_sta_idx + col_len; j++)
        {
            ifs >> minR[cnt];
            //cout << "minR " << minR[cnt] << endl;
            cnt++;
        }
        //cout << endl;
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

double CalcRMSE()
{
    printf("calc RMSE...\n");
    double rmse = 0;
    int cnt = 0;
    map<long, double>::iterator iter;
    int positve_cnt = 0;
    int negative_cnt = 0;
    for (iter = TestMap.begin(); iter != TestMap.end(); iter++)
    {
        long real_hash_idx = iter->first;
        long row_idx = real_hash_idx / M;
        long col_idx = real_hash_idx % M;
        double sum = 0;
        for (int k = 0; k < K; k++)
        {
            sum += P[row_idx][k] * Q[k][col_idx];
        }
        if (positve_cnt > iter->second)
        {
            positve_cnt++;
        }
        else
        {
            negative_cnt++;
            printf("sum = %lf  real=%lf\n", sum, iter->second );
        }
        rmse += (sum - iter->second) * (sum - iter->second);
        cnt++;
    }

    rmse /= cnt;
    rmse = sqrt(rmse);
    printf("positve_cnt=%d negative_cnt=%d\n", positve_cnt, negative_cnt );
    return rmse;
}
void partitionP(int portion_num,  Block* Pblocks)
{
    int i = 0;
    int height = N / portion_num;
    int last_height = N - (portion_num - 1) * height;

    for (i = 0; i < portion_num; i++)
    {
        Pblocks[i].block_id = i;
        Pblocks[i].data_age = 0;
        Pblocks[i].eles.clear();
        Pblocks[i].height = height;
        int sta_idx = i * height;
        if ( i == portion_num - 1)
        {
            Pblocks[i].height = last_height;
        }
        Pblocks[i].sta_idx = sta_idx;

        for (int h = 0; h < Pblocks[i].height; h++)
        {
            for (int j = 0; j < K; j++)
            {
                Pblocks[i].eles.push_back(P[h][j]);
            }
        }
        Pblocks[i].ele_num = Pblocks[i].eles.size();
    }

}

void partitionQ(int portion_num,  Block* Qblocks)
{
    int i = 0;
    int height = M / portion_num;
    int last_height = M - (portion_num - 1) * height;

    for (i = 0; i < portion_num; i++)
    {
        Qblocks[i].block_id = i;
        Qblocks[i].data_age = 0;
        Qblocks[i].eles.clear();
        Qblocks[i].height = height;
        int sta_idx = i * height;
        if ( i == portion_num - 1)
        {
            Qblocks[i].height = last_height;
        }
        Qblocks[i].sta_idx = sta_idx;

        for (int h = 0; h < Qblocks[i].height; h++)
        {
            for (int j = 0; j < K; j++)
            {
                Qblocks[i].eles.push_back(Q[j][h]);
            }
        }
        Qblocks[i].ele_num = Qblocks[i].eles.size();
    }

}