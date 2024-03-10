use std::{collections::HashMap, time::Duration};
use substring::Substring;
use thiserror::Error;
use tracing::info;

use r2d2_redis::{
    r2d2,
    redis::{Commands, FromRedisValue},
    RedisConnectionManager,
};

use super::DBHandler;
use crate::error::OreoError;

pub type R2D2Pool = r2d2::Pool<RedisConnectionManager>;
pub type R2D2Con = r2d2::PooledConnection<RedisConnectionManager>;

const CACHE_POOL_MAX_OPEN: u32 = 200;
const CACHE_POOL_MIN_IDLE: u32 = 8;
const CACHE_POOL_TIMEOUT_SECONDS: u64 = 3;
const CACHE_POOL_EXPIRE_SECONDS: u64 = 1;

pub const REDIS_ACCOUNT_KEY: &str = "IRONACCOUNT";

#[derive(Error, Debug)]
pub enum R2D2Error {
    #[error("could not get redis connection from pool : {0}")]
    RedisPoolError(r2d2_redis::r2d2::Error),
    #[error("error parsing string from redis result: {0}")]
    RedisTypeError(r2d2_redis::redis::RedisError),
    #[error("error executing redis command: {0}")]
    RedisCMDError(r2d2_redis::redis::RedisError),
    #[error("error creating Redis client: {0}")]
    RedisClientError(r2d2_redis::redis::RedisError),
}

#[derive(Debug, Clone)]
pub struct RedisClient {
    pub pool: r2d2::Pool<RedisConnectionManager>,
}

impl RedisClient {
    pub fn connect(url: &str) -> Result<Self, R2D2Error> {
        let manager = RedisConnectionManager::new(url).map_err(R2D2Error::RedisClientError)?;
        let pool = r2d2::Pool::builder()
            .max_size(CACHE_POOL_MAX_OPEN)
            .max_lifetime(Some(Duration::from_secs(CACHE_POOL_EXPIRE_SECONDS)))
            .min_idle(Some(CACHE_POOL_MIN_IDLE))
            .build(manager)
            .map_err(|e| R2D2Error::RedisPoolError(e).into())?;
        Ok(Self { pool })
    }

    pub fn get_con(&self) -> Result<R2D2Con, R2D2Error> {
        self.pool
            .get_timeout(Duration::from_secs(CACHE_POOL_TIMEOUT_SECONDS))
            .map_err(|e| {
                eprintln!("error connecting to redis: {}", e);
                R2D2Error::RedisPoolError(e).into()
            })
    }

    pub fn set_str(&self, key: &str, value: &str, ttl_seconds: usize) -> Result<(), R2D2Error> {
        let mut con = self.get_con()?;
        con.set(key, value).map_err(R2D2Error::RedisCMDError)?;
        if ttl_seconds > 0 {
            con.expire(key, ttl_seconds)
                .map_err(R2D2Error::RedisCMDError)?;
        }
        Ok(())
    }

    pub fn hset(&self, key: &str, field: &str, value: &str) -> Result<(), R2D2Error> {
        let mut con = self.get_con()?;
        con.hset(key, field, value)
            .map_err(R2D2Error::RedisCMDError)?;
        Ok(())
    }

    pub fn hget(&self, key: &str, field: &str) -> Result<String, R2D2Error> {
        let mut con = self.get_con()?;
        let val = con.hget(key, field).map_err(R2D2Error::RedisCMDError)?;
        FromRedisValue::from_redis_value(&val).map_err(|e| R2D2Error::RedisTypeError(e).into())
    }

    pub fn hgetall(&self, key: &str) -> Result<HashMap<String, String>, R2D2Error> {
        let mut con = self.get_con()?;
        let vals: HashMap<String, String> = con.hgetall(key).map_err(R2D2Error::RedisCMDError)?;
        Ok(vals)
    }

    pub fn hdel(&self, key: &str, field: &str) -> Result<(), R2D2Error> {
        let mut con = self.get_con()?;
        con.hdel(key, field).map_err(R2D2Error::RedisCMDError)?;
        Ok(())
    }

    pub fn get_str(&self, key: &str) -> Result<String, R2D2Error> {
        let mut con = self.get_con()?;
        let value = con.get(key).map_err(R2D2Error::RedisCMDError)?;
        FromRedisValue::from_redis_value(&value).map_err(|e| R2D2Error::RedisTypeError(e).into())
    }
}

impl DBHandler for RedisClient {
    fn init(db_addr: &str) -> Self {
        info!("Redis handler selected");
        Self::connect(db_addr).unwrap()
    }

    fn save_account(&self, address: String, _worker_id: u32) -> Result<String, OreoError> {
        match self.hget(REDIS_ACCOUNT_KEY, &address) {
            Ok(_) => {
                return Err(OreoError::Duplicate(address));
            }
            Err(e) => match e {
                R2D2Error::RedisPoolError(_) => return Err(OreoError::DBError),
                _ => info!("Ready to save new account, {}", address),
            },
        }
        let account_name = address_to_name(&address);
        if let Err(_) = self.hset(REDIS_ACCOUNT_KEY, &address, &account_name) {
            return Err(OreoError::DBError);
        }
        info!(
            "New account saved in redis, name: {}, address: {}",
            account_name, address
        );
        Ok(account_name)
    }

    fn get_account(&self, address: String) -> Result<String, OreoError> {
        match self.hget(REDIS_ACCOUNT_KEY, &address) {
            Ok(name) => Ok(name),
            Err(e) => match e {
                R2D2Error::RedisPoolError(_) => Err(OreoError::DBError),
                _ => Err(OreoError::NoImported(address)),
            },
        }
    }

    fn remove_account(&self, address: String) -> Result<String, OreoError> {
        match self.hget(REDIS_ACCOUNT_KEY, &address) {
            Ok(name) => {
                // should never panic
                self.hdel(REDIS_ACCOUNT_KEY, &address).unwrap();
                Ok(name)
            }
            Err(e) => match e {
                R2D2Error::RedisPoolError(_) => Err(OreoError::DBError),
                _ => Err(OreoError::NoImported(address)),
            },
        }
    }
}

pub fn address_to_name(address: &str) -> String {
    address.substring(0, 10).into()
}
