package cn.supcon;


import cn.supcon.utils.RedisUtil;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.io.IOException;
import java.util.*;


@SpringBootApplication
@RestController
public class App {

    @Autowired
    private TransportClient client;

    @Autowired
    private RedisTemplate redisTemplate;

    @RequestMapping("/")
    public String home() {
        RedisUtil redisUtil = new RedisUtil();
        redisUtil.setRedisTemplate(redisTemplate);
        Map user1 = new HashMap();
        user1.put("name", "BlockOne");
        user1.put("age", 12);
        redisUtil.set("user1", user1);
        Object ob = redisUtil.get("user1");
        System.out.println(ob);
        return "你好，这里是图书管理系统！";
    }

    // 根据ID查找文档
    @GetMapping("/get/people/man")
    @ResponseBody
    public ResponseEntity get(@RequestParam(name = "id", defaultValue = "") String id) {
        if (id.isEmpty()) {
            return new ResponseEntity(HttpStatus.NOT_FOUND);
        }
        GetResponse result = this.client.prepareGet("people", "man", id)
                .get();
        if (!result.isExists()) {
            return new ResponseEntity(HttpStatus.NOT_FOUND);
        }
        return new ResponseEntity(result.getSource(), HttpStatus.OK);
    }

    // 增加文档
    @PostMapping("/add/book/novel")
    @ResponseBody
    public ResponseEntity add(@RequestParam(name = "title") String title,
                              @RequestParam(name = "author") String author,
                              @RequestParam(name = "wordCount") int wordCount,
                              @RequestParam(name = "publishDate")
                                  @DateTimeFormat(pattern = "yyyy-MM-dd HH:mm:ss") Date publishDate) {
        try {
            XContentBuilder content = XContentFactory.jsonBuilder()
                    .startObject()
                    .field("title", title)
                    .field("author", author)
                    .field("word_count", wordCount)
                    .field("publish_date", publishDate)
                    .endObject();
            IndexResponse result = this.client.prepareIndex("book", "novel")
                    .setSource(content)
                    .get();
            return new ResponseEntity(result.getId(), HttpStatus.OK);
        } catch (IOException e) {
            return new ResponseEntity(HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }

    // 删除文档
    @DeleteMapping
    @ResponseBody
    public ResponseEntity delete(@RequestParam(name = "id") String id) {
        DeleteResponse result = this.client.prepareDelete("book", "novel", id)
                .get();
        return new ResponseEntity(result.getId(), HttpStatus.OK);
    }


    // 更新文档
@PutMapping("/update/book/novel")
@ResponseBody
public ResponseEntity update(@RequestParam(name = "id")String id,
                             @RequestParam(name = "title", required = false)String title,
                             @RequestParam(name = "author", required = false)String author) {
    UpdateRequest update = new UpdateRequest("book", "novel", id);
    try {
        XContentBuilder builder = XContentFactory.jsonBuilder()
                .startObject();
        if (title != null) {
            builder.field("title",title);
        }
        if (author != null) {
            builder.field("author", author);
        }
        builder.endObject();
        update.doc(builder);
        return new ResponseEntity(HttpStatus.OK);
    } catch (IOException e) {
        return new ResponseEntity(HttpStatus.INTERNAL_SERVER_ERROR);
    }
}

    // 高级查询
@PostMapping("/query/book/novel")
@ResponseBody
public ResponseEntity query(@RequestParam(name = "author", required = false)String author,
                            @RequestParam(name = "title", required = false)String title,
                            @RequestParam(name = "gtWordCount", defaultValue = "0")int gtWordCount,
                            @RequestParam(name = "ltWordCount", required = false) Integer ltWordCount){
    BoolQueryBuilder boolQuery = QueryBuilders.boolQuery();
    if (author != null) {
         boolQuery.must(QueryBuilders.matchQuery("author", author));
    }
    if (title != null) {
        boolQuery.must(QueryBuilders.matchQuery("title", title));
    }
    RangeQueryBuilder rangeQuery = QueryBuilders.rangeQuery("word_count")
            .from(gtWordCount);
    if (ltWordCount != null) {
        rangeQuery.to(ltWordCount);
    }
    boolQuery.filter(rangeQuery);
    SearchRequestBuilder builder = this.client.prepareSearch("book")
            .setTypes("novel")
            .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
            .setFrom(0)
            .setSize(10);
    SearchResponse response = builder.get();
    List<Map<String, Object>> result = new ArrayList<Map<String, Object>>();

    for (SearchHit hit : response.getHits()) {
        result.add(hit.getSource());
    }

    return new ResponseEntity(result, HttpStatus.OK);
}



    public static void main(String[] args) {
        System.out.println("Hello World!");
        SpringApplication.run(App.class, args);
    }
}
