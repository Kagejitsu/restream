//! Parsing of RTMP publish URLs.
//!
//! Streaming platforms hand out a *server URL* (`rtmp://live.twitch.tv/app`)
//! and a *stream key* separately; some tools accept a single combined URL
//! (`rtmps://live-api-s.facebook.com:443/rtmp/FB-123?ds=1`). Both forms are
//! accepted here. The last path segment of a combined URL is the stream key;
//! everything before it is the RTMP "application" name.

use std::fmt;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PublishUrl {
    pub secure: bool,
    pub host: String,
    pub port: u16,
    /// RTMP application (may contain slashes, e.g. `live2` or `app/foo`).
    pub app: String,
    /// Stream key / stream name, including any query string the platform requires.
    pub stream_key: String,
}

#[derive(Debug, thiserror::Error)]
pub enum UrlError {
    #[error("invalid URL: {0}")]
    Invalid(#[from] url::ParseError),
    #[error("scheme must be rtmp:// or rtmps://, got {0}")]
    Scheme(String),
    #[error("URL has no host")]
    NoHost,
    #[error("URL has no application path (expected rtmp://host/app)")]
    NoApp,
    #[error("stream key is empty")]
    NoKey,
}

impl PublishUrl {
    /// Parse a server URL (`rtmp://host[:port]/app`) and a separate stream key.
    pub fn from_server_and_key(server: &str, key: &str) -> Result<Self, UrlError> {
        let key = key.trim();
        if key.is_empty() {
            return Err(UrlError::NoKey);
        }
        let (secure, host, port, path) = split(server)?;
        let app = path.trim_matches('/').to_string();
        if app.is_empty() {
            return Err(UrlError::NoApp);
        }
        Ok(Self { secure, host, port, app, stream_key: key.to_string() })
    }

    /// Parse a combined URL whose last path segment is the stream key.
    pub fn from_combined(full: &str) -> Result<Self, UrlError> {
        let (secure, host, port, path) = split(full)?;
        let path = path.trim_start_matches('/');
        let Some((app, key)) = path.rsplit_once('/') else {
            return Err(UrlError::NoApp);
        };
        if key.is_empty() {
            return Err(UrlError::NoKey);
        }
        if app.is_empty() {
            return Err(UrlError::NoApp);
        }
        Ok(Self { secure, host, port, app: app.to_string(), stream_key: key.to_string() })
    }

    /// The `tcUrl` sent in the RTMP `connect` command.
    pub fn tc_url(&self) -> String {
        let scheme = if self.secure { "rtmps" } else { "rtmp" };
        let default = if self.secure { 443 } else { 1935 };
        if self.port == default {
            format!("{scheme}://{}/{}", self.host, self.app)
        } else {
            format!("{scheme}://{}:{}/{}", self.host, self.port, self.app)
        }
    }

    /// A display form that never reveals the stream key.
    pub fn redacted(&self) -> String {
        format!("{}/<key>", self.tc_url())
    }
}

impl fmt::Display for PublishUrl {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.redacted())
    }
}

/// Split into (secure, host, port, path-with-query).
fn split(s: &str) -> Result<(bool, String, u16, String), UrlError> {
    let u = url::Url::parse(s.trim())?;
    let secure = match u.scheme() {
        "rtmp" => false,
        "rtmps" => true,
        other => return Err(UrlError::Scheme(other.to_string())),
    };
    let host = u.host_str().ok_or(UrlError::NoHost)?.to_string();
    let port = u.port().unwrap_or(if secure { 443 } else { 1935 });
    let mut path = u.path().to_string();
    if let Some(q) = u.query() {
        path.push('?');
        path.push_str(q);
    }
    Ok((secure, host, port, path))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn twitch_server_and_key() {
        let u = PublishUrl::from_server_and_key("rtmp://live.twitch.tv/app", "live_123_abc").unwrap();
        assert_eq!(u.app, "app");
        assert_eq!(u.stream_key, "live_123_abc");
        assert_eq!(u.port, 1935);
        assert!(!u.secure);
        assert_eq!(u.tc_url(), "rtmp://live.twitch.tv/app");
    }

    #[test]
    fn facebook_combined_rtmps_with_query() {
        let u = PublishUrl::from_combined("rtmps://live-api-s.facebook.com:443/rtmp/FB-1?ds=1&s_sw=0").unwrap();
        assert!(u.secure);
        assert_eq!(u.port, 443);
        assert_eq!(u.app, "rtmp");
        assert_eq!(u.stream_key, "FB-1?ds=1&s_sw=0");
        assert_eq!(u.tc_url(), "rtmps://live-api-s.facebook.com/rtmp");
    }

    #[test]
    fn youtube_nested_app() {
        let u = PublishUrl::from_combined("rtmp://a.rtmp.youtube.com/live2/abcd-efgh").unwrap();
        assert_eq!(u.app, "live2");
        assert_eq!(u.stream_key, "abcd-efgh");
    }

    #[test]
    fn custom_port_is_kept_in_tc_url() {
        let u = PublishUrl::from_server_and_key("rtmp://127.0.0.1:1936/live", "k").unwrap();
        assert_eq!(u.tc_url(), "rtmp://127.0.0.1:1936/live");
    }

    #[test]
    fn rejects_bad_scheme_and_missing_parts() {
        assert!(matches!(PublishUrl::from_server_and_key("http://x/app", "k"), Err(UrlError::Scheme(_))));
        assert!(matches!(PublishUrl::from_server_and_key("rtmp://x", "k"), Err(UrlError::NoApp)));
        assert!(matches!(PublishUrl::from_server_and_key("rtmp://x/app", " "), Err(UrlError::NoKey)));
        assert!(matches!(PublishUrl::from_combined("rtmp://x/onlykey"), Err(UrlError::NoApp)));
    }

    #[test]
    fn display_never_leaks_key() {
        let u = PublishUrl::from_server_and_key("rtmp://h/app", "SECRET").unwrap();
        assert!(!u.to_string().contains("SECRET"));
    }
}
