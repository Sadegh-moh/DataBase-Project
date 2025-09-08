CREATE TABLE product (
  product_id   TEXT PRIMARY KEY,
  title        TEXT,
  price        DOUBLE PRECISION
);

CREATE TABLE app_user (
  user_id      TEXT PRIMARY KEY,
  profile_name TEXT
);

CREATE TABLE review (
  review_id            TEXT PRIMARY KEY,
  product_id           TEXT REFERENCES product(product_id),
  user_id              TEXT REFERENCES app_user(user_id),
  score                DOUBLE PRECISION,
  helpfulness_raw      TEXT,
  helpfulness_ratio    DOUBLE PRECISION,
  time_epoch           BIGINT,
  summary              TEXT,
  text                 TEXT
);

CREATE INDEX idx_review_score ON review(score);
CREATE INDEX idx_review_helpfulness ON review(helpfulness_ratio);
CREATE INDEX idx_review_product ON review(product_id);
CREATE INDEX idx_review_user ON review(user_id);
