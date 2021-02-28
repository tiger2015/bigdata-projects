package com.tiger.hadoop.topn;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

/**
 * @Author Zenghu
 * @Date 2021/2/28 20:28
 * @Description
 * @Version: 1.0
 **/
public class Blog implements WritableComparable<Blog> {
    private String uuid;
    private String publishDate; // 发表日期
    private int likes = 0; // 点赞数
    private int comments = 0; // 评论数
    private int relays = 0; // 转发数
    private String author; // 作者
    private int follows = 0;// 关注数
    private int fans = 0; // 粉丝数
    private String blogerSex; // 博主性别
    private String content;


    public String getUuid() {
        return uuid;
    }

    public void setUuid(String uuid) {
        this.uuid = uuid;
    }

    public String getPublishDate() {
        return publishDate;
    }

    public void setPublishDate(String publishDate) {
        this.publishDate = publishDate;
    }

    public int getLikes() {
        return likes;
    }

    public void setLikes(int likes) {
        this.likes = likes;
    }

    public int getComments() {
        return comments;
    }

    public void setComments(int comments) {
        this.comments = comments;
    }

    public int getRelays() {
        return relays;
    }

    public void setRelays(int relays) {
        this.relays = relays;
    }

    public String getAuthor() {
        return author;
    }

    public void setAuthor(String author) {
        this.author = author;
    }

    public int getFollows() {
        return follows;
    }

    public void setFollows(int follows) {
        this.follows = follows;
    }

    public int getFans() {
        return fans;
    }

    public void setFans(int fans) {
        this.fans = fans;
    }

    public String getBlogerSex() {
        return blogerSex;
    }

    public void setBlogerSex(String blogerSex) {
        this.blogerSex = blogerSex;
    }

    public String getContent() {
        return content;
    }

    public void setContent(String content) {
        this.content = content;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(uuid);
        out.writeUTF(publishDate);
        out.writeInt(likes);
        out.writeInt(comments);
        out.writeInt(relays);
        out.writeUTF(author);
        out.writeInt(follows);
        out.writeInt(fans);
        out.writeUTF(blogerSex);
        out.writeUTF(content);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        uuid = in.readUTF();
        publishDate = in.readUTF();
        likes = in.readInt();
        comments = in.readInt();
        relays = in.readInt();
        author = in.readUTF();
        follows = in.readInt();
        fans = in.readInt();
        blogerSex = in.readUTF();
        content = in.readUTF();
    }

    @Override
    public int compareTo(Blog o) {
        // 根据粉丝数量排序(降序排列)
        return o.fans - fans;
    }

    @Override
    public String toString() {
        return uuid + "\t" + author + "\t" + fans;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Blog blog = (Blog) o;
        return Objects.equals(uuid, blog.uuid) &&
                Objects.equals(publishDate, blog.publishDate) &&
                Objects.equals(author, blog.author);
    }

    @Override
    public int hashCode() {
        return Objects.hash(uuid, publishDate, author);
    }
}
