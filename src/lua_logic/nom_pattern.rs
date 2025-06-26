use nom::{
    branch::alt,
    bytes::complete::take_while1,
    character::complete::char,
    combinator::{map, opt},
    multi::separated_list1,
    sequence::{delimited, preceded},
    IResult,
};

#[derive(Debug, Clone)]
pub struct PathPattern<'a> {
    pub pattern: &'a str,
    pub segments: Vec<PatternSegment<'a>>,
}

#[derive(Debug, Clone)]
pub enum PatternSegment<'a> {
    Named(&'a str),   //
    Wildcard,         // *
    Literal(&'a str), // literal text
}

#[derive(Debug, Clone)]
pub enum ExtractedSegment<'a> {
    Named(&'a str, &'a str),   // (name, matched_value)
    Wildcard(Vec<&'a str>),    // matched path segments
    Literal(&'a str, &'a str), // (expected, matched_value)
}

impl<'a> PathPattern<'a> {
    pub fn new(pattern: &'a str) -> Result<Self, anyhow::Error> {
        Self::new_with_nom(pattern)
    }

    pub fn parse_path(&self, path: &'a str) -> Result<Vec<ExtractedSegment>, anyhow::Error> {
        match self.parse_all_segments(path) {
            Ok((remaining, extracted)) => {
                if !remaining.is_empty() {
                    return Err(anyhow::anyhow!("Unparsed input: '{}'", remaining));
                }
                Ok(extracted)
            }
            Err(e) => Err(anyhow::anyhow!("Parse failed: {}", e)),
        }
    }

    fn parse_all_segments(&self, input: &'a str) -> IResult<&str, Vec<ExtractedSegment>> {
        // Split on '/' and parse each segment
        let (remaining, raw_segments) = preceded(
            opt(char('/')),
            separated_list1(char('/'), take_while1(|c: char| c != '/')),
        )(input)?;

        // Match raw_segments against self.segments pattern
        let mut results = Vec::new();

        for (i, segment) in self.segments.iter().enumerate() {
            match segment {
                PatternSegment::Named(name) => {
                    if i >= raw_segments.len() {
                        return Err(nom::Err::Error(nom::error::Error::new(
                            input,
                            nom::error::ErrorKind::Eof,
                        )));
                    }
                    results.push(ExtractedSegment::Named(name, raw_segments[i]));
                }
                PatternSegment::Literal(literal) => {
                    if i >= raw_segments.len() {
                        return Err(nom::Err::Error(nom::error::Error::new(
                            input,
                            nom::error::ErrorKind::Eof,
                        )));
                    }
                    if raw_segments[i] != *literal {
                        return Err(nom::Err::Error(nom::error::Error::new(
                            input,
                            nom::error::ErrorKind::Tag,
                        )));
                    }
                    results.push(ExtractedSegment::Literal(literal, raw_segments[i]));
                }
                PatternSegment::Wildcard => {
                    results.push(ExtractedSegment::Wildcard(raw_segments[i..].to_vec()));
                    break; // Wildcard consumes everything
                }
            }
        }

        // if there are still segments then the last item was not a wildcard
        // but we can just capture everything anyways as it will always be correct
        let processed_count = self.segments.len();
        if processed_count < raw_segments.len() {
            let remaining_segments = raw_segments[processed_count..].to_vec();
            results.push(ExtractedSegment::Wildcard(remaining_segments));
        }

        Ok((remaining, results))
    }
}

fn parse_named(input: &str) -> IResult<&str, PatternSegment> {
    map(
        delimited(
            char('{'),
            take_while1(|c: char| c.is_alphanumeric() || c == '_'),
            char('}'),
        ),
        |name: &str| PatternSegment::Named(name),
    )(input)
}

fn parse_wildcard(input: &str) -> IResult<&str, PatternSegment> {
    map(char('*'), |_| PatternSegment::Wildcard)(input)
}

fn parse_literal(input: &str) -> IResult<&str, PatternSegment> {
    map(
        take_while1(|c: char| c != '/' && c != '{' && c != '}' && c != '*'),
        |literal: &str| PatternSegment::Literal(literal),
    )(input)
}

// Parse a single segment
fn parse_segment(input: &str) -> IResult<&str, PatternSegment> {
    alt((parse_named, parse_wildcard, parse_literal))(input)
}

// Parse the full pattern
fn parse_pattern(input: &str) -> IResult<&str, Vec<PatternSegment>> {
    separated_list1(char('/'), parse_segment)(input)
}

impl<'a> PathPattern<'a> {
    pub fn new_with_nom(pattern: &'a str) -> Result<Self, anyhow::Error> {
        let cleaned_pattern = pattern.trim_start_matches('/');

        match parse_pattern(cleaned_pattern) {
            Ok((remaining, segments)) => {
                if !remaining.is_empty() {
                    return Err(anyhow::anyhow!(
                        "Failed to parse entire pattern: '{}'",
                        remaining
                    ));
                }
                Ok(PathPattern { pattern, segments })
            }
            Err(e) => Err(anyhow::anyhow!("Pattern parsing failed: {}", e)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_path_pattern_simple() {
        let pattern = PathPattern::new("{tenant}/{schema_table}").unwrap();

        assert_eq!(pattern.segments.len(), 2);
        assert!(matches!(pattern.segments[0], PatternSegment::Named(_)));
        assert!(matches!(pattern.segments[1], PatternSegment::Named(_)));
    }

    #[test]
    fn test_path_pattern_with_wildcard() {
        let pattern = PathPattern::new("{tenant}/{schema_table}/*").unwrap();

        assert_eq!(pattern.segments.len(), 3);
        assert!(matches!(pattern.segments[2], PatternSegment::Wildcard));
    }

    #[test]
    fn test_path_pattern_with_literal() {
        let pattern = PathPattern::new("{tenant}/data/{schema_table}").unwrap();

        assert_eq!(pattern.segments.len(), 3);
        if let PatternSegment::Literal(literal) = pattern.segments[1] {
            assert_eq!(literal, "data");
        } else {
            panic!("Expected literal segment");
        }
    }

    #[test]
    fn test_parse_path_basic() {
        let pattern = PathPattern::new("{tenant}/{schema_table}").unwrap();
        let path = "/tenant_name/schema.table/file.parquet";

        let extracted = pattern.parse_path(path).unwrap();

        assert_eq!(extracted.len(), 3);
        match extracted[0] {
            ExtractedSegment::Named(name, value) => {
                assert_eq!(name, "tenant");
                assert_eq!(value, "tenant_name");
            }
            _ => panic!("Expected Named segment"),
        }
        match extracted[1] {
            ExtractedSegment::Named(name, value) => {
                assert_eq!(name, "schema_table");
                assert_eq!(value, "schema.table");
            }
            _ => panic!("Expected Named segment"),
        }
    }

    #[test]
    fn test_parse_path_with_wildcard() {
        let pattern = PathPattern::new("{tenant}/{schema_table}/{load_type}/*").unwrap();
        let path = "/tenant_name/schema.table/delta/2025/06/25/file.snappy.parquet";

        let extracted = pattern.parse_path(path).unwrap();

        assert_eq!(extracted.len(), 5);
        match extracted[0] {
            ExtractedSegment::Named(name, value) => {
                assert_eq!(name, "tenant");
                assert_eq!(value, "tenant_name");
            }
            _ => panic!("Expected Named segment"),
        }
        match &extracted[3] {
            ExtractedSegment::Wildcard(segments) => {
                assert_eq!(segments, &vec!["2025", "06", "25", "file.snappy.parquet"]);
            }
            _ => panic!("Expected Wildcard segment"),
        }
    }

    #[test]
    fn test_parse_path_with_literal() {
        let pattern = PathPattern::new("{tenant}/data/{schema_table}").unwrap();
        let path = "/tenant_name/data/schema.table/file.parquet";

        let extracted = pattern.parse_path(path).unwrap();

        assert_eq!(extracted.len(), 4);
        match extracted[1] {
            ExtractedSegment::Literal(expected, matched) => {
                assert_eq!(expected, "data");
                assert_eq!(matched, "data");
            }
            _ => panic!("Expected Literal segment"),
        }
    }
}
