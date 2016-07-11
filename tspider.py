#!/usr/bin/env python
#coding:utf-8
import urllib2,random,urllib,re,sys,time,datetime
from datetime import timedelta
from tornado import httpclient,gen,ioloop,queues
from pyquery import PyQuery as pq
import motor.motor_tornado
import motor
try:
    from urlparse import urljoin,urldefrag
except ImportError:
    from urllib.parse import urljoin,urldefrag

__metaclass__=type

client = motor.motor_tornado.MotorClient('localhost',27017)
db = client['fundb']
textFun = db['textFun']#文本内容
userdb = db["allUsers"]#用户内容
concurrency=10
debug=False
baseurl="http://www.jokeji.cn/"
def get_links(resp):
    nlinks=[]
    html = pq(resp)
    lis= html(".list_title ul li")
    links=lis.find("a")
    print "\n当前获取的链接数量是:%s"%len(links)
    for i in xrange(len(links)):
        print "\ncurrent url is appended:%s"%urljoin(baseurl,links.eq(i).attr("href"))
        nlinks.append(urljoin(baseurl,links.eq(i).attr("href")))
    return nlinks

@gen.coroutine
def parse_url(url):
    title=""
    content=[]
    try:
        response = yield httpclient.AsyncHTTPClient().fetch(url)
        time.sleep(random.choice(range(2)))
        sys.stdout.write("\n"+"fetching url:%s..."%url)
        resp = response.body if isinstance(response.body,str) else response.body.decode()
        html=pq(resp)
        d=html("#text110")
        alist=html("h1").text().split("->")
        title=alist[2]
        topic=alist[1]
        ps=d.find("p")
        if len(ps)>0:
            for i in range(len(ps)):
                sys.stdout.write("\n"+"current text is:")
                print ps.eq(i).text()
                content.append(ps.eq(i).text())
        else:
            sys.stdout.write("\n"+"no para for this entry,parse html and save to content.")
            content.append(d.html())
        sys.stdout.write("\n"+"get current page title:%s"%title)
        sys.stdout.write("\n"+"get length of current text is :%s"%len(content))
        yield insertText(title,content,topic)
        sys.stdout.write("\n"+"inserting into mongodb asynchronously")
    except IndexError as e:
        sys.stdout.write("\n"+"parsing url got error as:%s"%e)
        pass
@gen.coroutine    
def insertText(title,text,topic=""):
    try:
        fid="tf"+random.choice(list("textFun"))+str(int(time.time()*1000))
        ttime=datetime.datetime.now().strftime('%m/%d/%Y %H:%M:%S')#'06/10/2016 15:13:56'
        doc={"_id":fid,"title":title,"ntext":text,"ttime":ttime,"user_like":0,"comment_count":0,"author":"","topic":topic}
        future=textFun.insert(doc)
        result = yield future#wait for insert operation to be completed
        sys.stdout.write("\n"+"got result as:%s"%result)
    except Exception as e:
        sys.stdout.write("\n"+"catching exception as:%s"%e)
    
@gen.coroutine
def crawl_links_from_url():
    alinks=[]
    try:
        curls=[urljoin(baseurl,i) for i in ["list34_%s.htm"%i for i in xrange(1,8)]]        
        for url in curls:
            response = yield httpclient.AsyncHTTPClient().fetch(url)
            time.sleep(random.choice(range(3)))
            print "\nfetched %s"%url
            resp = response.body if isinstance(response.body,str) else response.body.decode()
            nlinks=get_links(resp)
            alinks+=nlinks
    except Exception as e:
        sys.stdout.write("\n"+"catch exception as:%s"%e)
        pass
    raise gen.Return(alinks)

@gen.coroutine
def main():
    q=queues.Queue()
    start=time.time()
    fetching,fetched =set(),set()
    if(not debug):
        urls = yield crawl_links_from_url()
        for new_url in urls:
            yield q.put(new_url)
    @gen.coroutine
    def execute_parse():
        try:
            current_url = yield q.get()
            if current_url in fetching:
                return
            print "currently fetching url:%s"%current_url
            fetching.add(current_url)
            yield parse_url(current_url)
            fetched.add(current_url)
        except Exception as e:
            sys.stdout.write("\nwhen parsing url catch exception:%s"%e)
            pass
        finally:
            q.task_done()
    @gen.coroutine
    def worker():
        while True:
            yield execute_parse()

    #q.put(baseurl+"jokehtml/bxnn/20080925160600.htm")
    #start works
    for _ in range(concurrency):
        worker()
    yield q.join(timeout=timedelta(seconds=30000))#when queue is empty
    print len(fetching)
    print len(fetched)
    assert fetching == fetched
    print "\njobs done!,took %s seconds"%(time.time()-start)

if __name__ =="__main__":
    ioloop.IOLoop.current().run_sync(main)


