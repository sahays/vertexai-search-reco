-- SQL to fix JSON string fields in BigQuery table
-- Run this in BigQuery console to create a cleaned version of your data

CREATE OR REPLACE TABLE `your-project.dataset.table_fixed` AS
SELECT
  id,
  title,
  desc,
  short_desc,
  original_title,
  audio_lang,
  primary_language,
  age_rating,
  image,
  keywords,
  season_count,
  episode_count,
  asset_type,
  c_releasedate,
  release_date,
  licensing_from,
  licensing_until,
  
  -- Convert JSON strings back to arrays
  CASE 
    WHEN genre IS NULL OR genre = '' OR genre = '[]' THEN []
    ELSE JSON_EXTRACT_ARRAY(genre) 
  END as genre,
  
  CASE 
    WHEN category IS NULL OR category = '' OR category = '[]' THEN []
    ELSE JSON_EXTRACT_ARRAY(category) 
  END as category,
  
  CASE 
    WHEN content_language IS NULL OR content_language = '' OR content_language = '[]' THEN []
    ELSE JSON_EXTRACT_ARRAY(content_language) 
  END as content_language,
  
  CASE 
    WHEN actors IS NULL OR actors = '' OR actors = '[]' THEN []
    ELSE JSON_EXTRACT_ARRAY(actors) 
  END as actors,
  
  CASE 
    WHEN directors IS NULL OR directors = '' OR directors = '[]' THEN []
    ELSE JSON_EXTRACT_ARRAY(directors) 
  END as directors,
  
  CASE 
    WHEN tags IS NULL OR tags = '' OR tags = '[]' THEN []
    ELSE JSON_EXTRACT_ARRAY(tags) 
  END as tags,
  
  CASE 
    WHEN subtitle_lang IS NULL OR subtitle_lang = '' OR subtitle_lang = '[]' THEN []
    ELSE JSON_EXTRACT_ARRAY(subtitle_lang) 
  END as subtitle_lang,
  
  CASE 
    WHEN rights IS NULL OR rights = '' OR rights = '[]' THEN []
    ELSE JSON_EXTRACT_ARRAY(rights) 
  END as rights,
  
  -- Convert JSON strings back to objects
  CASE 
    WHEN country_meta IS NULL OR country_meta = '' OR country_meta = '{}' THEN NULL
    ELSE PARSE_JSON(country_meta) 
  END as country_meta,
  
  CASE 
    WHEN extended IS NULL OR extended = '' OR extended = '{}' THEN NULL
    ELSE PARSE_JSON(extended) 
  END as extended

FROM `your-project.dataset.table_original`;