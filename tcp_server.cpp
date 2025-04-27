#include <assert.h>
#include <stdint.h>
#include <stdlib.h>
#include <string>
#include <stdio.h>
#include <errno.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <netinet/ip.h>
#include <fcntl.h>
#include <poll.h>
#include <vector>
#include <cstring>
#include <map>

#include "hashtable.h"
#include "hashtable.cpp"

#define container_of(ptr, type, member) \
    ((type *)((char *)(ptr) - offsetof(type, member)))


typedef vector<uint8_t> Buffer;
using namespace std;


const size_t max_msg= 32<<20; 


static void msg(const char *msg) {
    fprintf(stderr, "%s\n", msg);
}

static void msg_err(const char *msg){
    fprintf(stderr, "[errno:%d] %s\n", errno, msg);
}

static void die(const char *msg) {
    int err = errno;
    fprintf(stderr, "[%d] %s\n", err, msg);
    abort();
}

static void fd_set_nb(int fd) {
    errno = 0;
    int flags = fcntl(fd, F_GETFL, 0);
    if (errno) {
        die("fcntl error");
        return;
    }

    flags |= O_NONBLOCK;

    errno = 0;
    (void)fcntl(fd, F_SETFL, flags);
    if (errno) {
        die("fcntl error");
    }
}

struct Conn {
    int fd= -1;
    bool want_read= false;
    bool want_write= false;
    bool want_close= false;

    vector<uint8_t> incoming;
    vector<uint8_t> outgoing;

};

static void buf_append(vector<uint8_t> &buf,const uint8_t *data, size_t len){
    buf.insert(buf.end(),data, data+len);
}
static void buf_consume(vector<uint8_t> &buf, size_t n){
    buf.erase(buf.begin(),buf.begin()+n);
}

// static int32_t read_full(int fd, char* buff, size_t n){

//     while(n>0){
//         ssize_t rv= read(fd, buff, n);
//         if(rv<=0){
//             return -1;
//         }
//         assert((size_t)rv<=n);
//         n-=(size_t)rv;
//         buff+=rv;
//     }
//     return 0;
// }


// static int32_t write_all(int fd,const char* buff, size_t n){

//     while(n>0){
//         ssize_t rv= write(fd, buff, n);
//         if(rv<=0){
//             return -1;
//         }
//         assert((size_t)rv<=n);
//         n-=(size_t)rv;
//         buff+=rv;
//     }
//     return 0;
// }


static Conn *handle_accept(int fd) {

    struct sockaddr_in client_addr = {};
    socklen_t addrlen = sizeof(client_addr);
    int connfd = accept(fd, (struct sockaddr *)&client_addr, &addrlen);
    if (connfd < 0) {
        msg_err("accept() error");
        return NULL;
    }
    uint32_t ip = client_addr.sin_addr.s_addr;
    fprintf(stderr, "new client from %u.%u.%u.%u:%u\n",
        ip & 255, (ip >> 8) & 255, (ip >> 16) & 255, ip >> 24,
        ntohs(client_addr.sin_port)
    );

    // set the new connection fd to nonblocking mode
    fd_set_nb(connfd);

    // create a `struct Conn`
    Conn *conn = new Conn();
    conn->fd = connfd;
    conn->want_read = true;
    return conn;
}

const size_t max_args= 200*1000;

static bool read_u32(const uint8_t *&cur,const uint8_t *end, uint32_t &out) {
    if (cur +4>end) {
        return false; // not enough space
    }
    memcpy(&out, cur, 4);
    cur+=4;
    return true;
}

static bool read_str(const uint8_t *&cur,const uint8_t *end,size_t n, string &out) {
    if (cur +n>end) {
        return false; // not enough space
    }
    out.assign(cur,cur+n);
    cur+=n;
    return true;
}


static int32_t parse_req(const uint8_t *data, size_t size,vector<string> &out) {

    const uint8_t *end = data + size;
    uint32_t nstr=0;
    if (!read_u32(data, end, nstr)) {
        return -1; // not enough space
    }
    if (nstr > max_args) {
        return -1; // too many args
    }
    
    while(out.size() < nstr) {
        uint32_t len = 0;
        if (!read_u32(data, end, len)) {
            return -1; 
        }
        out.push_back(string());
        if (!read_str(data, end, len, out.back())) {
            return -1; 
        }
    }

    if(data!= end){
        return -1; // not enough space
    }
    return 0;
}



enum {
    ERR_UNKNOWN=1,
    ERR_TOO_LONG=2,
};

enum {
    TAG_NIL = 0, 
    TAG_ERR = 1,   
    TAG_STR = 2,   
    TAG_INT = 3,    
    TAG_DBL = 4,    
    TAG_ARR = 5,    
};

static void buf_append_u8(Buffer &buf, uint8_t v) {
    buf.push_back(v);
}
static void buf_append_u32(Buffer &buf, uint32_t v) {
    buf_append(buf,(const uint8_t *)&v, 4);
}
static void buf_append_i64(Buffer &buf, int64_t v) {
    buf_append(buf,(const uint8_t *)&v, 8);
}
static void buf_append_dbl(Buffer &buf, double v) {
    buf_append(buf,(const uint8_t *)&v, 8);
}

static void out_nil(Buffer &out){
    buf_append_u8(out,TAG_NIL);
}

static void out_err(Buffer &out, uint32_t errcode, const string &msg) {
    buf_append_u8(out,TAG_ERR);
    buf_append_u32(out,errcode);
    buf_append_u32(out,(uint32_t)msg.size());
    buf_append(out,(const uint8_t *)msg.data(), msg.size());
}

static void out_str(Buffer &out, const char *msg,size_t len) {
    buf_append_u8(out,TAG_STR);
    buf_append_u32(out,(uint32_t)len);
    buf_append(out,(const uint8_t *)msg, len);
}

static void out_int(Buffer &out, int64_t v) {
    buf_append_u8(out,TAG_INT);
    buf_append_i64(out,v);
}

static void out_dbl(Buffer &out, double v) {
    buf_append_u8(out,TAG_DBL);
    buf_append_dbl(out,v);
}

static void out_arr(Buffer &out, uint32_t n) {
    buf_append_u8(out,TAG_ARR);
    buf_append_u32(out,n);
}

static struct {
    HashMap db;
} g_data;

struct Entry {
    struct Node node;
    string key;
    string val;
};

static bool entry_eq(Node *a, Node *b) {
    struct Entry *ea = container_of(a,struct Entry, node);
    struct Entry *eb = container_of(b,struct Entry, node);
    return ea->key == eb->key;
}

static uint64_t str_hash(const uint8_t *data, size_t len) {
    uint32_t h= 0x811C9DC5;
    for(size_t i=0;i<len;i++){
        h=(h+data[i])*0x01000193;
    }
    return h;
}


static void do_get(vector<string> &cmd, Buffer &out) {
    Entry key;
    key.key.swap(cmd[1]);
    key.node.hash = str_hash((const uint8_t *)key.key.data(), key.key.size());
    Node *node = hm_lookup(&g_data.db, &key.node, &entry_eq);

    if (node == NULL) {
        return out_nil(out); // not found
    }

    const string &val = container_of(node, Entry, node)->val;
    assert(val.size() < max_msg);
    return out_str(out, val.data(), val.size());
}

static void do_set(vector<string> &cmd, Buffer &out) {
    Entry key;
    key.key.swap(cmd[1]);
    key.node.hash = str_hash((const uint8_t *)key.key.data(), key.key.size());

    Node *node = hm_lookup(&g_data.db, &key.node, &entry_eq);

    if(node){
        container_of(node, Entry, node)->val.swap(cmd[2]);
    }
    else{
        Entry *new_entry = new Entry();
        new_entry->key.swap(key.key);
        new_entry->val.swap(cmd[2]);
        new_entry->node.hash = key.node.hash;
        hm_insert(&g_data.db, &new_entry->node);
    }
    return out_nil(out); // not found
}

static void do_del(vector<string> &cmd, Buffer &out) {
    Entry key;
    key.key.swap(cmd[1]);
    key.node.hash = str_hash((const uint8_t *)key.key.data(), key.key.size());
    Node *node = hm_delete(&g_data.db, &key.node, &entry_eq);

    if (node) {
        delete container_of(node, Entry, node);
    }
    return out_int(out,node? 1:0);
}

static bool cb_keys(Node *node, void *arg) {
    Buffer &out = *(Buffer *)arg;
    const string &key = container_of(node, Entry, node)->key;
    out_str(out, key.data(), key.size());
    return true;
}

static void do_keys(vector<string> & , Buffer &out) {
    out_arr(out, (uint32_t)hm_size(&g_data.db));
    hm_foreach(&g_data.db, &cb_keys, (void *)&out);
}

static void do_request(vector<string> &cmd, Buffer &out) {
    if (cmd.size() == 2 && cmd[0] == "get") {
        return do_get(cmd, out);
    }else if (cmd.size() == 3 && cmd[0] == "set") {
        return do_set(cmd, out);
        
    } else if (cmd.size()== 2 && cmd[0]=="del")
    {
        return do_del(cmd, out);
    }
    else if (cmd.size() == 1 && cmd[0] == "keys") {
        return do_keys(cmd, out);
    }
    else {
        return out_err(out, ERR_UNKNOWN, "unknown command");
    }
    
}

// static void make_response(const Response &resp, vector<uint8_t> &out) {
//     uint32_t len =4+ (uint32_t)resp.data.size();
//     buf_append(out, (const uint8_t *)&len, 4);
//     buf_append(out, (const uint8_t *)&resp.status, 4);
//     buf_append(out, resp.data.data(), resp.data.size());
// }

static void response_begin(Buffer &out, size_t *header){
    *header = out.size();
    buf_append_u32(out, 0); // placeholder for length}
}

static size_t response_size(Buffer &out, size_t header) {
    return out.size() - header - 4;
}

static void response_end(Buffer &out, size_t header) {
    size_t len = response_size(out, header);
    if(len > max_msg) {
        out.resize(header + 4); // reset to header + 4
        out_err(out, ERR_TOO_LONG, "too long response");
        len = response_size(out, header);
    }
    uint32_t x =(uint32_t)len;
    memcpy(&out[header], &x, 4); // write the length
}

static bool try_one_request(Conn *conn) {
    
    if (conn->incoming.size() < 4) {
        return false;   
    }
    uint32_t len = 0;
    memcpy(&len, conn->incoming.data(), 4);
    if (len > max_msg) {
        msg("too long");
        conn->want_close = true;
        return false;   
    }
    
    if (4 + len > conn->incoming.size()) {
        return false;   
    }
    const uint8_t *request = &conn->incoming[4];

    vector<string> cmd;
    if(parse_req(request, len, cmd) < 0) {
        msg("parse error");
        conn->want_close = true;
        return false;   
    }

    size_t header = 0;
    response_begin(conn->outgoing, &header);
    do_request(cmd, conn->outgoing);
    response_end(conn->outgoing, header);

    buf_consume(conn->incoming, 4 + len);
    return true;        
}


static void handle_write(Conn *conn) {
    assert(conn->outgoing.size() > 0);
    ssize_t rv = write(conn->fd, &conn->outgoing[0], conn->outgoing.size());
    if (rv < 0 && errno == EAGAIN) {
        return; 
    }
    if (rv < 0) {
        msg_err("write() error");
        conn->want_close = true;  
        return;
    }

    
    buf_consume(conn->outgoing, (size_t)rv);

    
    if (conn->outgoing.size() == 0) {   
        conn->want_read = true;
        conn->want_write = false;
    } 
}



static void handle_read(Conn *conn) {

    uint8_t buf[64 * 1024];
    ssize_t rv = read(conn->fd, buf, sizeof(buf));
    if (rv < 0 && errno == EAGAIN) {
        return; 
    }
  
    if (rv < 0) {
        msg_err("read() error");
        conn->want_close = true;
        return; 
    }
   
    if (rv == 0) {
        if (conn->incoming.size() == 0) {
            msg("client closed");
        } else {
            msg("unexpected EOF");
        }
        conn->want_close = true;
        return; 
    }
    
    buf_append(conn->incoming, buf, (size_t)rv);

    
    while (try_one_request(conn)) {}

    
    if (conn->outgoing.size() > 0) {   
        conn->want_read = false;
        conn->want_write = true;
        // The socket is likely ready to write in a request-response protocol,
        // try to write it without waiting for the next iteration.
        return handle_write(conn);
    }   // else: want read
}


// static int32_t _request(int connfd) {
//     char rbuf[4+ max_msg];
//     errno=0;
//     int32_t err = read_full(connfd, rbuf, 4);
//     if (err) {
//         msg(errno== 0? "EOF" : "read() error");
//         return err;
//     }

//     uint32_t len =0;
//     memcpy(&len, rbuf,4);
//     if(len> max_msg){
//         msg("too long");
//         return -1;
//     }

//     err = read_full(connfd, &rbuf[4], len);
//     if (err) {
//         msg("read() error");
//         return err;
//     }

//     fprintf(stderr, "client says: %.*s\n", len, &rbuf[4]);

//     const char reply[] = "world";
//     char wbuf[4 + sizeof(reply)];
//     len = (uint32_t)strlen(reply);
//     memcpy(wbuf, &len, 4);
//     memcpy(&wbuf[4], reply, len);
//     return write_all(connfd, wbuf, 4 + len);
// }

int main() {
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) {
        die("socket()");
    }


    int val = 1;
    setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &val, sizeof(val));


    struct sockaddr_in addr = {};
    addr.sin_family = AF_INET;
    addr.sin_port = ntohs(1234);
    addr.sin_addr.s_addr = ntohl(0);   
    int rv = bind(fd, (const struct sockaddr *)&addr, sizeof(addr));
    if (rv) {
        die("bind()");
    }

    fd_set_nb(fd);

    rv = listen(fd, SOMAXCONN);
    if (rv) {
        die("listen()");
    }

    vector<Conn *> fd2conn;
    vector<struct pollfd> poll_args;

    while (true) {

        poll_args.clear();

        struct pollfd pfd = {fd, POLLIN, 0};
        poll_args.push_back(pfd);


        for (Conn *conn : fd2conn) {
            if (!conn) {
                continue;
            }
            
            struct pollfd pfd = {conn->fd, POLLERR, 0};
            
            if (conn->want_read) {
                pfd.events |= POLLIN;
            }
            if (conn->want_write) {
                pfd.events |= POLLOUT;
            }
            poll_args.push_back(pfd);
        }

        // struct sockaddr_in client_addr = {};
        // socklen_t addrlen = sizeof(client_addr);
        // int connfd = accept(fd, (struct sockaddr *)&client_addr, &addrlen);
        // if (connfd < 0) {
        //     continue;   
        // }

        int rv = poll(poll_args.data(), (nfds_t)poll_args.size(), -1);
        if (rv < 0 && errno == EINTR) {
            continue;   // not an error
        }
        if (rv < 0) {
            die("poll");
        }


        if (poll_args[0].revents) {
            if (Conn *conn = handle_accept(fd)) {
                // put it into the map
                if (fd2conn.size() <= (size_t)conn->fd) {
                    fd2conn.resize(conn->fd + 1);
                }
                assert(!fd2conn[conn->fd]);
                fd2conn[conn->fd] = conn;
            }
        }

        
        // while(true){
            
        //     int32_t err= _request(connfd);
            
        //     if(err) break;

        // }
        // close(connfd);


        for (size_t i = 1; i < poll_args.size(); ++i) { // note: skip the 1st
            uint32_t ready = poll_args[i].revents;
            if (ready == 0) {
                continue;
            }

            Conn *conn = fd2conn[poll_args[i].fd];
            if (ready & POLLIN) {
                assert(conn->want_read);
                handle_read(conn);  // application logic
            }
            if (ready & POLLOUT) {
                assert(conn->want_write);
                handle_write(conn); // application logic
            }

            // close the socket from socket error or application logic
            if ((ready & POLLERR) || conn->want_close) {
                (void)close(conn->fd);
                fd2conn[conn->fd] = NULL;
                delete conn;
            }
        }   
    }

    return 0;
}