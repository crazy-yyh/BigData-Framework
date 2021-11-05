package com.yuhang;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.http.HttpHost;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.CreateIndexResponse;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.client.indices.GetIndexResponse;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.SortOrder;

import java.io.IOException;

/**
 * @author yyh
 * @create 2021-10-16 23:04
 */
public class Es_Client {

    public static void main(String[] args) throws IOException {
        // 创建ES客户端
        RestHighLevelClient esClient = new RestHighLevelClient(
            RestClient.builder(new HttpHost("localhost", 9200, "http"))
        );


          // 创建索引
//        CreateIndexRequest request = new CreateIndexRequest("user");
//        CreateIndexResponse createIndexResponse =
//            esClient.indices().create(request, RequestOptions.DEFAULT);
//
//        // 响应状态
//        boolean acknowledged = createIndexResponse.isAcknowledged();
//        System.out.println("索引操作 ：" + acknowledged);



        // 查询索引
//        GetIndexRequest request = new GetIndexRequest("user");
//
//        GetIndexResponse getIndexResponse =
//            esClient.indices().get(request, RequestOptions.DEFAULT);
//
//        // 响应状态
//        System.out.println(getIndexResponse.getAliases());
//        System.out.println(getIndexResponse.getMappings());
//        System.out.println(getIndexResponse.getSettings());


        //删除索引
//        DeleteIndexRequest request = new DeleteIndexRequest("user");
//
//        AcknowledgedResponse response = esClient.indices().delete(request, RequestOptions.DEFAULT);
//
//        // 响应状态
//        System.out.println(response.isAcknowledged());


        // 插入数据
//        IndexRequest request = new IndexRequest();
//        request.index("user").id("1001");
//
//        User user = new User();
//        user.setName("zhangsan");
//        user.setAge(30);
//        user.setSex("男");
//
//        // 向ES插入数据，必须将数据转换位JSON格式
//        ObjectMapper mapper = new ObjectMapper();
//        String userJson = mapper.writeValueAsString(user);
//        request.source(userJson, XContentType.JSON);
//
//        IndexResponse response = esClient.index(request, RequestOptions.DEFAULT);
//
//        System.out.println(response.getResult());



        // 修改数据
//        UpdateRequest request = new UpdateRequest();
//        request.index("user").id("1001");
//        request.doc(XContentType.JSON, "sex", "女");
//
//        UpdateResponse response = esClient.update(request, RequestOptions.DEFAULT);
//
//        System.out.println(response.getResult());


        //查询文档数据
//        GetRequest request = new GetRequest();
//        request.index("user").id("1001");
//        GetResponse response = esClient.get(request, RequestOptions.DEFAULT);
//
//        System.out.println(response.getSourceAsString());




        //删除文档数据
//        DeleteRequest request = new DeleteRequest();
//        request.index("user").id("1001");
//
//        DeleteResponse response = esClient.delete(request, RequestOptions.DEFAULT);
//        System.out.println(response.toString());



        // 批量插入数据
//        BulkRequest request = new BulkRequest();
//
//        request.add(new IndexRequest().index("user").id("1001").source(XContentType.JSON, "name", "zhangsan", "age", 30, "sex", "男"));
//        request.add(new IndexRequest().index("user").id("1002").source(XContentType.JSON, "name", "lisi", "age", 30, "sex", "女"));
//        request.add(new IndexRequest().index("user").id("1003").source(XContentType.JSON, "name", "wangwu", "age", 40, "sex", "男"));
//        request.add(new IndexRequest().index("user").id("1006").source(XContentType.JSON, "name", "wangwu3", "age",50,"sex","男"));
//        request.add(new IndexRequest().index("user").id("1007").source(XContentType.JSON, "name", "wangwu44", "age",60,"sex","男"));
//        request.add(new IndexRequest().index("user").id("1008").source(XContentType.JSON, "name", "wangwu555", "age",60,"sex","男"));
//
//        BulkResponse response = esClient.bulk(request, RequestOptions.DEFAULT);
//        System.out.println(response.getTook());
//        System.out.println(response.getItems());


        // 批量删除数据
//        BulkRequest request = new BulkRequest();
//
//        request.add(new DeleteRequest().index("user").id("1001"));
//        request.add(new DeleteRequest().index("user").id("1002"));
//        request.add(new DeleteRequest().index("user").id("1003"));
//
//        BulkResponse response = esClient.bulk(request, RequestOptions.DEFAULT);
//        System.out.println(response.getTook());
//        System.out.println(response.getItems());


        //  1. 查询索引中全部的数据
//        SearchRequest request = new SearchRequest();
//        request.indices("user");
//
//        request.source(new SearchSourceBuilder().query(QueryBuilders.matchAllQuery()));
//
//        SearchResponse response = esClient.search(request, RequestOptions.DEFAULT);
//
//        SearchHits hits = response.getHits();
//
//        System.out.println(hits.getTotalHits());
//        System.out.println(response.getTook());
//
//        for (SearchHit hit : hits) {
//            System.out.println(hit.getSourceAsString());
//        }

        // 2. 条件查询 : termQuery
//        SearchRequest request = new SearchRequest();
//        request.indices("user");
//
//        request.source(new SearchSourceBuilder().query(QueryBuilders.termQuery("age", 30)));
//        SearchResponse response = esClient.search(request, RequestOptions.DEFAULT);
//
//        SearchHits hits = response.getHits();
//
//        System.out.println(hits.getTotalHits());
//        System.out.println(response.getTook());
//
//        for (SearchHit hit : hits) {
//            System.out.println(hit.getSourceAsString());
//        }

        // 3. 分页查询
//        SearchRequest request = new SearchRequest();
//        request.indices("user");
//
//        SearchSourceBuilder builder = new SearchSourceBuilder().query(QueryBuilders.matchAllQuery());
//        // (当前页码-1)*每页显示数据条数
//        builder.from(2);
//        builder.size(2);
//        request.source(builder);
//        SearchResponse response = esClient.search(request, RequestOptions.DEFAULT);
//
//        SearchHits hits = response.getHits();
//
//        System.out.println(hits.getTotalHits());
//        System.out.println(response.getTook());
//
//        for (SearchHit hit : hits) {
//            System.out.println(hit.getSourceAsString());
//        }

        // 4. 查询排序
//        SearchRequest request = new SearchRequest();
//        request.indices("user");
//
//        SearchSourceBuilder builder = new SearchSourceBuilder().query(QueryBuilders.matchAllQuery());
//        //
//        builder.sort("age", SortOrder.DESC);
//
//        request.source(builder);
//        SearchResponse response = esClient.search(request, RequestOptions.DEFAULT);
//
//        SearchHits hits = response.getHits();
//
//        System.out.println(hits.getTotalHits());
//        System.out.println(response.getTook());
//
//        for (SearchHit hit : hits) {
//            System.out.println(hit.getSourceAsString());
//        }

        // 5. 过滤字段
//        SearchRequest request = new SearchRequest();
//        request.indices("user");
//
//        SearchSourceBuilder builder = new SearchSourceBuilder().query(QueryBuilders.matchAllQuery());
//        //
//        String[] excludes = {"age"};
//        String[] includes = {};
//        builder.fetchSource(includes, excludes);
//
//        request.source(builder);
//        SearchResponse response = esClient.search(request, RequestOptions.DEFAULT);
//
//        SearchHits hits = response.getHits();
//
//        System.out.println(hits.getTotalHits());
//        System.out.println(response.getTook());
//
//        for (SearchHit hit : hits) {
//            System.out.println(hit.getSourceAsString());
//        }

        // 6. 组合查询
//        SearchRequest request = new SearchRequest();
//        request.indices("user");
//
//        SearchSourceBuilder builder = new SearchSourceBuilder();
//        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
//
//        //boolQueryBuilder.must(QueryBuilders.matchQuery("age", 30));
//        //boolQueryBuilder.must(QueryBuilders.matchQuery("sex", "男"));
//        //boolQueryBuilder.mustNot(QueryBuilders.matchQuery("sex", "男"));
//        boolQueryBuilder.should(QueryBuilders.matchQuery("age", 30));
//        boolQueryBuilder.should(QueryBuilders.matchQuery("age", 40));
//
//        builder.query(boolQueryBuilder);
//
//        request.source(builder);
//        SearchResponse response = esClient.search(request, RequestOptions.DEFAULT);
//
//        SearchHits hits = response.getHits();
//
//        System.out.println(hits.getTotalHits());
//        System.out.println(response.getTook());
//
//        for (SearchHit hit : hits) {
//            System.out.println(hit.getSourceAsString());
//        }

        //        // 7. 范围查询
        //        SearchRequest request = new SearchRequest();
        //        request.indices("user");
        //
        //        SearchSourceBuilder builder = new SearchSourceBuilder();
        //        RangeQueryBuilder rangeQuery = QueryBuilders.rangeQuery("age");
        //
        //        rangeQuery.gte(30);
        //        rangeQuery.lt(50);
        //
        //        builder.query(rangeQuery);
        //
        //        request.source(builder);
        //        SearchResponse response = esClient.search(request, RequestOptions.DEFAULT);
        //
        //        SearchHits hits = response.getHits();
        //
        //        System.out.println(hits.getTotalHits());
        //        System.out.println(response.getTook());
        //
        //        for ( SearchHit hit : hits ) {
        //            System.out.println(hit.getSourceAsString());
        //        }

        // 8. 模糊查询
        //        SearchRequest request = new SearchRequest();
        //        request.indices("user");
        //
        //        SearchSourceBuilder builder = new SearchSourceBuilder();
        //        builder.query(QueryBuilders.fuzzyQuery("name", "wangwu").fuzziness(Fuzziness.TWO));
        //
        //        request.source(builder);
        //        SearchResponse response = esClient.search(request, RequestOptions.DEFAULT);
        //
        //        SearchHits hits = response.getHits();
        //
        //        System.out.println(hits.getTotalHits());
        //        System.out.println(response.getTook());
        //
        //        for ( SearchHit hit : hits ) {
        //            System.out.println(hit.getSourceAsString());
        //        }

        //        // 9. 高亮查询
        //        SearchRequest request = new SearchRequest();
        //        request.indices("user");
        //
        //        SearchSourceBuilder builder = new SearchSourceBuilder();
        //        TermsQueryBuilder termsQueryBuilder = QueryBuilders.termsQuery("name", "zhangsan");
        //
        //        HighlightBuilder highlightBuilder = new HighlightBuilder();
        //        highlightBuilder.preTags("<font color='red'>");
        //        highlightBuilder.postTags("</font>");
        //        highlightBuilder.field("name");
        //
        //        builder.highlighter(highlightBuilder);
        //        builder.query(termsQueryBuilder);
        //
        //        request.source(builder);
        //        SearchResponse response = esClient.search(request, RequestOptions.DEFAULT);
        //
        //        SearchHits hits = response.getHits();
        //
        //        System.out.println(hits.getTotalHits());
        //        System.out.println(response.getTook());
        //
        //        for ( SearchHit hit : hits ) {
        //            System.out.println(hit.getSourceAsString());
        //        }

        //        // 10. 聚合查询
        //        SearchRequest request = new SearchRequest();
        //        request.indices("user");
        //
        //        SearchSourceBuilder builder = new SearchSourceBuilder();
        //
        //        AggregationBuilder aggregationBuilder = AggregationBuilders.max("maxAge").field("age");
        //        builder.aggregation(aggregationBuilder);
        //
        //        request.source(builder);
        //        SearchResponse response = esClient.search(request, RequestOptions.DEFAULT);
        //
        //        SearchHits hits = response.getHits();
        //
        //        System.out.println(hits.getTotalHits());
        //        System.out.println(response.getTook());
        //
        //        for ( SearchHit hit : hits ) {
        //            System.out.println(hit.getSourceAsString());
        //        }

        // 11. 分组查询
//        SearchRequest request = new SearchRequest();
//        request.indices("user");
//
//        SearchSourceBuilder builder = new SearchSourceBuilder();
//
//        AggregationBuilder aggregationBuilder = AggregationBuilders.terms("ageGroup").field("age");
//        builder.aggregation(aggregationBuilder);
//
//        request.source(builder);
//        SearchResponse response = esClient.search(request, RequestOptions.DEFAULT);
//
//        SearchHits hits = response.getHits();
//
//        System.out.println(hits.getTotalHits());
//        System.out.println(response.getTook());
//
//        for ( SearchHit hit : hits ) {
//            System.out.println(hit.getSourceAsString());
//        }





        // 关闭ES客户端
        esClient.close();
    }
}
