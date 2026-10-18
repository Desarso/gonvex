/* Gonvex ingress fast path. Every unsupported string returns SQL NULL so the
** owner uses its existing ECMAScript lowercase implementation for the whole
** value, preserving contextual casing, expansions and new Unicode scripts. */
#include "sqlite3.h"
static void gv_fast_lower(sqlite3_context *context,int argc,sqlite3_value **argv){
  const unsigned char *text;unsigned char *out;int count,at=0,used=0;
  (void)argc;
  if(sqlite3_value_type(argv[0])==SQLITE_NULL){sqlite3_result_text(context,"",0,SQLITE_STATIC);return;}
  text=sqlite3_value_text(argv[0]);count=sqlite3_value_bytes(argv[0]);
  if(!text){sqlite3_result_error_nomem(context);return;}
  out=sqlite3_malloc64((sqlite3_uint64)count+1);
  if(!out){sqlite3_result_error_nomem(context);return;}
  while(at<count){
    unsigned int a=text[at],cp;int width;
    if(a<128){out[used++]=(unsigned char)(a>=65&&a<=90?a+32:a);at++;continue;}
    if(a>=194&&a<=195){
      if(at+2>count||(text[at+1]&192)!=128)goto fallback;
      cp=((a&31)<<6)|(text[at+1]&63);
      if((cp>=192&&cp<=214)||(cp>=216&&cp<=222))cp+=32;
      out[used++]=(unsigned char)(192|(cp>>6));out[used++]=(unsigned char)(128|(cp&63));at+=2;continue;
    }
    width=3;
    if(a<224||a>239||at+width>count||(text[at+1]&192)!=128||(text[at+2]&192)!=128)goto fallback;
    cp=((a&15)<<12)|((text[at+1]&63)<<6)|(text[at+2]&63);
    /* Unified CJK ideographs and Japanese kana have no case mappings. */
    if(!((cp>=0x4e00&&cp<=0x9fff)||(cp>=0x3040&&cp<=0x30ff)))goto fallback;
    out[used++]=text[at++];out[used++]=text[at++];out[used++]=text[at++];
  }
  out[used]=0;sqlite3_result_text64(context,(const char *)out,(sqlite3_uint64)used,sqlite3_free,SQLITE_UTF8);return;
fallback:
  sqlite3_free(out);sqlite3_result_null(context);
}
static void gv_fast_contract(sqlite3_context *context,int argc,sqlite3_value **argv){(void)argc;(void)argv;sqlite3_result_text(context,"ascii-latin1-cjk-v1",-1,SQLITE_STATIC);}
/* SHA-256 over exact ingress bytes. Canonical row encoding remains the wire
** contract; this function does not normalize, reorder or reinterpret JSON. */
#include <stdint.h>
#include <string.h>
typedef struct gv_sha256_state {uint32_t h[8];uint64_t bytes;unsigned int used;unsigned char block[64];} gv_sha256_state;
static uint32_t gv_rotr(uint32_t x,unsigned int n){return (x>>n)|(x<<(32-n));}
static void gv_sha256_block(gv_sha256_state *s,const unsigned char *p){
 static const uint32_t k[64]={
  0x428a2f98,0x71374491,0xb5c0fbcf,0xe9b5dba5,0x3956c25b,0x59f111f1,0x923f82a4,0xab1c5ed5,
  0xd807aa98,0x12835b01,0x243185be,0x550c7dc3,0x72be5d74,0x80deb1fe,0x9bdc06a7,0xc19bf174,
  0xe49b69c1,0xefbe4786,0x0fc19dc6,0x240ca1cc,0x2de92c6f,0x4a7484aa,0x5cb0a9dc,0x76f988da,
  0x983e5152,0xa831c66d,0xb00327c8,0xbf597fc7,0xc6e00bf3,0xd5a79147,0x06ca6351,0x14292967,
  0x27b70a85,0x2e1b2138,0x4d2c6dfc,0x53380d13,0x650a7354,0x766a0abb,0x81c2c92e,0x92722c85,
  0xa2bfe8a1,0xa81a664b,0xc24b8b70,0xc76c51a3,0xd192e819,0xd6990624,0xf40e3585,0x106aa070,
  0x19a4c116,0x1e376c08,0x2748774c,0x34b0bcb5,0x391c0cb3,0x4ed8aa4a,0x5b9cca4f,0x682e6ff3,
  0x748f82ee,0x78a5636f,0x84c87814,0x8cc70208,0x90befffa,0xa4506ceb,0xbef9a3f7,0xc67178f2};
 uint32_t w[64],a,b,c,d,e,f,g,h,t1,t2;unsigned int i;
 for(i=0;i<16;i++)w[i]=((uint32_t)p[i*4]<<24)|((uint32_t)p[i*4+1]<<16)|((uint32_t)p[i*4+2]<<8)|p[i*4+3];
 for(i=16;i<64;i++){
  uint32_t x=w[i-15],y=w[i-2];
  w[i]=w[i-16]+(gv_rotr(x,7)^gv_rotr(x,18)^(x>>3))+w[i-7]+(gv_rotr(y,17)^gv_rotr(y,19)^(y>>10));
 }
 a=s->h[0];b=s->h[1];c=s->h[2];d=s->h[3];e=s->h[4];f=s->h[5];g=s->h[6];h=s->h[7];
 for(i=0;i<64;i++){
  t1=h+(gv_rotr(e,6)^gv_rotr(e,11)^gv_rotr(e,25))+((e&f)^((~e)&g))+k[i]+w[i];
  t2=(gv_rotr(a,2)^gv_rotr(a,13)^gv_rotr(a,22))+((a&b)^(a&c)^(b&c));
  h=g;g=f;f=e;e=d+t1;d=c;c=b;b=a;a=t1+t2;
 }
 s->h[0]+=a;s->h[1]+=b;s->h[2]+=c;s->h[3]+=d;s->h[4]+=e;s->h[5]+=f;s->h[6]+=g;s->h[7]+=h;
}
static void gv_sha256_update(gv_sha256_state *s,const unsigned char *data,unsigned int count){
 s->bytes+=count;
 while(count){unsigned int take=64-s->used;if(take>count)take=count;memcpy(s->block+s->used,data,take);s->used+=take;data+=take;count-=take;if(s->used==64){gv_sha256_block(s,s->block);s->used=0;}}
}
static void gv_sha256(sqlite3_context *context,int argc,sqlite3_value **argv){
 static const char digits[]="0123456789abcdef";
 gv_sha256_state s={{0x6a09e667,0xbb67ae85,0x3c6ef372,0xa54ff53a,0x510e527f,0x9b05688c,0x1f83d9ab,0x5be0cd19},0,0,{0}};
 const unsigned char *input;unsigned int count,i;uint64_t bits;char result[65];(void)argc;
 if(sqlite3_value_type(argv[0])==SQLITE_NULL){sqlite3_result_null(context);return;}
 input=sqlite3_value_blob(argv[0]);count=(unsigned int)sqlite3_value_bytes(argv[0]);
 if(!input&&count){sqlite3_result_error_nomem(context);return;}
 gv_sha256_update(&s,input,count);bits=s.bytes*8;
 s.block[s.used++]=0x80;
 if(s.used>56){memset(s.block+s.used,0,64-s.used);gv_sha256_block(&s,s.block);s.used=0;}
 memset(s.block+s.used,0,56-s.used);
 for(i=0;i<8;i++)s.block[63-i]=(unsigned char)(bits>>(i*8));
 gv_sha256_block(&s,s.block);
 for(i=0;i<32;i++){unsigned char byte=(unsigned char)(s.h[i/4]>>(24-(i%4)*8));result[i*2]=digits[byte>>4];result[i*2+1]=digits[byte&15];}
 result[64]=0;sqlite3_result_text(context,result,64,SQLITE_TRANSIENT);
}
static void gv_hash_contract(sqlite3_context *context,int argc,sqlite3_value **argv){(void)argc;(void)argv;sqlite3_result_text(context,"sha256-v1",-1,SQLITE_STATIC);}
static int gv_install(sqlite3 *db,char **error,const sqlite3_api_routines *api){
  int rc;(void)error;(void)api;
  rc=sqlite3_create_function_v2(db,"gv_ingress_lower_fast",1,SQLITE_UTF8|SQLITE_DETERMINISTIC|SQLITE_DIRECTONLY,0,gv_fast_lower,0,0,0);
  if(rc==SQLITE_OK)rc=sqlite3_create_function_v2(db,"gv_ingress_fast_contract",0,SQLITE_UTF8|SQLITE_DETERMINISTIC|SQLITE_INNOCUOUS,0,gv_fast_contract,0,0,0);
  if(rc==SQLITE_OK)rc=sqlite3_create_function_v2(db,"gv_ingress_sha256",1,SQLITE_UTF8|SQLITE_DETERMINISTIC|SQLITE_DIRECTONLY,0,gv_sha256,0,0,0);
  if(rc==SQLITE_OK)rc=sqlite3_create_function_v2(db,"gv_ingress_hash_contract",0,SQLITE_UTF8|SQLITE_DETERMINISTIC|SQLITE_INNOCUOUS,0,gv_hash_contract,0,0,0);
  return rc;
}
int sqlite3_wasm_extra_init(const char *unused){(void)unused;return sqlite3_auto_extension((void(*)(void))gv_install);}
