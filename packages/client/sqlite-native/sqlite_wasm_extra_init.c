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
static int gv_install(sqlite3 *db,char **error,const sqlite3_api_routines *api){
  int rc;(void)error;(void)api;
  rc=sqlite3_create_function_v2(db,"gv_ingress_lower_fast",1,SQLITE_UTF8|SQLITE_DETERMINISTIC|SQLITE_DIRECTONLY,0,gv_fast_lower,0,0,0);
  if(rc==SQLITE_OK)rc=sqlite3_create_function_v2(db,"gv_ingress_fast_contract",0,SQLITE_UTF8|SQLITE_DETERMINISTIC|SQLITE_INNOCUOUS,0,gv_fast_contract,0,0,0);
  return rc;
}
int sqlite3_wasm_extra_init(const char *unused){(void)unused;return sqlite3_auto_extension((void(*)(void))gv_install);}
