library(ggplot2)
library(dplyr)
library(lubridate)
library(tidyr)
library(DT)
library(ggthemes)
library(wordcloud)
library(tm)
library(SnowballC)
library(corrplot)

# a) Read the YouTube stat from 
#    locations = CA, FR, GB, IN, US and prepare the data.

CA <- tail(read.csv("E:/R/CAvideos.csv", encoding = "UTF-8"), 20000)
FR <- tail(read.csv("E:/R/FRvideos.csv", encoding = "UTF-8"), 20000)
GB <- tail(read.csv("E:/R/GBvideos.csv", encoding = "UTF-8"), 20000)
IN <- tail(read.csv("E:/R/INvideos.csv", encoding = "UTF-8"), 20000)
US <- tail(read.csv("E:/R/USvideos.csv", encoding = "UTF-8"), 20000)

CA$trending_date <- ydm(CA$trending_date)
CA$publish_time <- ydm(substr(CA$publish_time, start = 0, stop = 8))
head(CA)

FR$trending_date <- ydm(FR$trending_date)
FR$publish_time <- ydm(substr(FR$publish_time, start = 0, stop = 8))
head(FR)

GB$trending_date <- ydm(GB$trending_date)
GB$publish_time <- ydm(substr(GB$publish_time, start = 1, stop = 8))
head(GB)

IN$trending_date <- ydm(IN$trending_date)
IN$publish_time <- ydm(substr(IN$publish_time, start = 0, stop = 8))
head(IN)

US$trending_date <- ydm(US$trending_date)
US$publish_time <- ydm(substr(US$publish_time, start = 0, stop = 8))
head(US)

# Row wise concatenation of records
YouTube <- rbind(CA,FR,GB,IN,US)
tail(YouTube)

sum(is.na(YouTube$publish_time))

# b. Display the correlation plot between category_id, 
#    views, likes, dislikes, comment_count. Which two
#    have stronger and weaker correlation

YouTube_df <- YouTube[, 8:11]
groups <- YouTube[, 5]
head(YouTube_df)

pairs(YouTube_df, labels = colnames(YouTube_df),
      pch = 21,
      bg = rainbow(4)[groups],
      col = rainbow(4)[groups])

corrplot(cor(YouTube_df), method = 'number')
corrplot(cor(YouTube_df), method = 'color')
corrplot(cor(YouTube_df), method = 'pie')

# c. Display Top 10 most viewed videos of YouTube.

top_10_viewed <- head(YouTube %>%
  group_by(video_id, title) %>%
  dplyr::summarise(Total = sum(views)) %>%
  arrange(desc(Total)), 10)

datatable(top_10_viewed)

ggplot(top_10_viewed, aes(video_id, Total)) +
  geom_bar( stat = "identity", fill = rainbow(10)) + 
  ylab("Total Views") +
  ggtitle("Top 10 Most Viewed Videos")

# d. Show Top 10 most liked videos on YouTube.

top_10_liked <- head(YouTube %>%
                      group_by(video_id, title) %>%
                      dplyr::summarise(Total = sum(likes)) %>%
                      arrange(desc(Total)), 10)

datatable(top_10_liked)

ggplot(top_10_liked, aes(video_id, Total)) +
  geom_bar( stat = "identity", fill = rainbow(10)) + 
  ylab("Total Likes") +
  ggtitle("Top 10 Most Liked Videos")

# e. Show Top 10 most disliked videos on YouTube.

top_10_disliked <- head(YouTube %>%
                       group_by(video_id, title) %>%
                       dplyr::summarise(Total = sum(dislikes)) %>%
                       arrange(desc(Total)), 10)

datatable(top_10_disliked)

ggplot(top_10_disliked, aes(video_id, Total)) +
  geom_bar( stat = "identity", fill = rainbow(10)) + 
  ylab("Total Dislikes") +
  ggtitle("Top 10 Most Disliked Videos")

# f. Show Top 10 most commented video of YouTube

top_10_commented <- head(YouTube %>%
                          group_by(video_id, title) %>%
                          dplyr::summarise(Total = sum(comment_count)) %>%
                          arrange(desc(Total)), 10)

datatable(top_10_commented)

ggplot(top_10_commented, aes(video_id, Total)) +
  geom_bar( stat = "identity", fill = rainbow(10)) + 
  ylab("Total Comments") +
  ggtitle("Top 10 Most Commented Videos")

# g. Show Top 15 videos with maximum percentage (%) of Likes on basis of views on video.
#    Hint: round (100* max (likes, na.rm = T)/ max (views, na.rm = T), digits = 2))

top_15_videos <- head(YouTube %>%
                           group_by(video_id, title) %>%
                           dplyr::summarise(Result = round(100*max(likes,na.rm=T)/max(views,na.rm=T),digits=2)) %>%
                           arrange(desc(Result)), 15)

datatable(top_15_videos)

ggplot(top_15_videos, aes(video_id, Result)) +
  geom_bar( stat = "identity", fill = rainbow(15)) + 
  ylab("Percentage of Likes") +
  ggtitle("Top 15 Videos With Maximum Percentage of Likes on Basis of Views")

# h. Show Top 15 videos with maximum percentage (%) of Dislikes on basis of views on video.

top_15_videos <- head(YouTube %>%
                        group_by(video_id, title) %>%
                        dplyr::summarise(Result = round(100*max(dislikes,na.rm=T)/max(views,na.rm=T),digits=2)) %>%
                        arrange(desc(Result)), 15)

datatable(top_15_videos)

ggplot(top_15_videos, aes(video_id, Result)) +
  geom_bar( stat = "identity", fill = rainbow(15)) + 
  ylab("Percentage of Dislikes") +
  ggtitle("Top 15 Videos With Maximum Percentage of Dislikes on Basis of Views")

# i. Show Top 15 videos with maximum percentage (%) of Comments on basis of views on video.

top_15_videos <- head(YouTube %>%
                        group_by(video_id, title) %>%
                        dplyr::summarise(Result = round(100*max(comment_count,na.rm=T)/max(views,na.rm=T),digits=2)) %>%
                        arrange(desc(Result)), 15)

datatable(top_15_videos)

ggplot(top_15_videos, aes(video_id, Result)) +
  geom_bar( stat = "identity", fill = "darkblue") + 
  ylab("Percentage of Comments") +
  ggtitle("Top 15 Videos With Maximum Percentage of Comments on Basis of Views")

# j. Top trending YouTube channels in all countries

top_trending_all <- head(YouTube %>%
                        group_by(channel_title) %>%
                        dplyr::summarise(Total = sum(views)) %>%
                        arrange(desc(Total)), 10)

datatable(top_trending_all)

ggplot(top_trending_all, aes(channel_title, Total)) +
  geom_bar( stat = "identity", fill = rainbow(10)) + 
  ylab("Total Views") +
  ggtitle("Top Trending YouTube Channels in All Countries")

# k. Top trending YouTube channels in India.

top_trending_IN <- head(IN %>%
                           group_by(channel_title) %>%
                           dplyr::summarise(Total = sum(views)) %>%
                           arrange(desc(Total)), 10)

datatable(top_trending_IN)

ggplot(top_trending_IN, aes(channel_title, Total)) +
  geom_bar( stat = "identity", fill = rainbow(10)) + 
  ylab("Total Views") +
  ggtitle("Top Trending YouTube Channels in India")

# l. Create a YouTube Title Wordcloud.

wordcloud(words = YouTube$title,
          max.words = 300,
          random.order = FALSE,
          rot.per = 0.4,
          colors = brewer.pal(8, "Dark2"))

# m. Show Top Category ID

top_category_id <- head(YouTube %>%
                           group_by(category_id) %>%
                           dplyr::summarise(Total = n()) %>%
                           arrange(desc(Total)), 10)

datatable(top_category_id)

ggplot(top_category_id, aes(category_id, Total)) +
  geom_bar( stat = "identity", fill = rainbow(10)) + 
  ylab("Count of Category ID") +
  ggtitle("Top Category ID")

# n. How much time passes between published and trending?

published_trending <- head(YouTube %>%
                          group_by(video_id) %>%
                          dplyr::summarise(Result = difftime(publish_time, trending_date, units = "days")) %>%
                          arrange(Result), 20)

head(published_trending, 20)

datatable(published_trending)

ggplot(published_trending, aes(video_id, Result)) +
  geom_bar( stat = "identity", fill = rainbow(20)) + 
  ylab("Difference") +
  scale_y_continuous() +
  ggtitle("Time Passes Between Published and Trending Date")

# o. Show the relationship plots between Views Vs. Likes on Youtube.

min_view <- min(YouTube$views)
max_view <- max(YouTube$views)
min_like <- min(YouTube$likes)
max_like <- max(YouTube$likes)

ggplot(YouTube, aes(x=views, y=likes)) +
  geom_point(size = 1, color = "maroon1") +
  scale_x_continuous(limits = c(min_view, max_view)) +
  scale_y_continuous(limits = c(min_like, max_like)) +
  ggtitle("Relationship Plots Between Views Vs. Likes")

# p. Top Countries In total number of Views in absolute numbers

Countries <- c("CA","FR","GB","IN","US")
Views <- c(sum(CA$views), sum(FR$views), sum(GB$views), 
           sum(IN$views), sum(US$views))
Likes <- c(sum(CA$likes), sum(FR$likes), sum(GB$likes), 
           sum(IN$likes), sum(US$likes))
Dislikes <- c(sum(CA$dislikes), sum(FR$dislikes), sum(GB$dislikes), 
              sum(IN$dislikes), sum(US$dislikes))
Comments <- c(sum(CA$comment_count), sum(FR$comment_count), sum(GB$comment_count), 
              sum(IN$comment_count), sum(US$comment_count))

new_df <- data.frame(Countries, Views, Likes, Dislikes, Comments)
new_df
top_country <- head(new_df %>%
                             arrange(desc(Views)))
datatable(top_country)

ggplot(top_country, aes(x=Countries, y=Views)) +
  geom_bar( stat = "identity", fill = "deeppink2") +
  ggtitle("Top Countries In Total Number of Views")

# q. Top Countries In total number of Likes in absolute numbers

top_like_country <- head(new_df %>%
                      arrange(desc(Likes)))
datatable(top_like_country)

ggplot(top_like_country, aes(x=Countries, y=Likes)) +
  geom_bar( stat = "identity", fill = "deeppink2") +
  ggtitle("Top Countries In Total Number of Likes")

# r. Top Countries In total number of Dislikes in absolute numbers

top_dislike_country <- head(new_df %>%
                           arrange(desc(Dislikes)))
datatable(top_dislike_country)

ggplot(top_dislike_country, aes(x=Countries, y=Dislikes)) +
  geom_bar( stat = "identity", fill = "deeppink2") +
  ggtitle("Top Countries In Total Number of Dislikes")

# s. Top Countries In total number of Comments in absolute numbers

top_comment_country <- head(new_df %>%
                              arrange(desc(Comments)))
datatable(top_comment_country)

ggplot(top_comment_country, aes(x=Countries, y=Comments)) +
  geom_bar( stat = "identity", fill = "deeppink2") +
  ggtitle("Top Countries In Total Number of Comments")

# t. Title length words Frequency Distribution

