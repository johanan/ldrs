use nom::{
    branch::alt,
    bytes::complete::{tag, take_until, take_while1},
    character::complete::char,
    combinator::{map, opt, rest},
    multi::{many1, separated_list1},
    sequence::{delimited, preceded},
    IResult,
};
use serde_json::{json, Value};

#[derive(Debug, Clone)]
pub struct PathPattern<'a> {
    pub pattern: &'a str,
    pub segments: Vec<PatternSegment<'a>>,
    pub segment_groups: Vec<Vec<PatternSegment<'a>>>,
}

#[derive(Debug, Clone)]
pub enum PatternSegment<'a> {
    Named(&'a str),   // {name}
    Wildcard,         // *
    Literal(&'a str), // literal text
    Placeholder,      // {_}
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

        // Map each path segment against its pattern segments, then flatten
        let mut results = Vec::new();

        for (i, segment_patterns) in self.segment_groups.iter().enumerate() {
            // Handle wildcard specially - it consumes all remaining segments
            if segment_patterns.len() == 1 {
                if let PatternSegment::Wildcard = segment_patterns[0] {
                    results.push(ExtractedSegment::Wildcard(raw_segments[i..].to_vec()));
                    break; // Wildcard consumes everything
                }
            }

            if i >= raw_segments.len() {
                return Err(nom::Err::Error(nom::error::Error::new(
                    input,
                    nom::error::ErrorKind::Eof,
                )));
            }

            let segment_results = self.parse_single_segment(raw_segments[i], segment_patterns)?;
            results.extend(segment_results); // Flatten here
        }

        Ok((remaining, results))
    }

    fn parse_single_segment(
        &self,
        segment: &'a str,
        patterns: &[PatternSegment<'a>],
    ) -> Result<Vec<ExtractedSegment<'a>>, nom::Err<nom::error::Error<&'a str>>> {
        // Handle simple cases first
        if patterns.len() == 1 {
            match &patterns[0] {
                PatternSegment::Named(name) => {
                    return Ok(vec![ExtractedSegment::Named(name, segment)]);
                }
                PatternSegment::Literal(literal) => {
                    if segment == *literal {
                        return Ok(vec![ExtractedSegment::Literal(literal, segment)]);
                    } else {
                        return Err(nom::Err::Error(nom::error::Error::new(
                            segment,
                            nom::error::ErrorKind::Tag,
                        )));
                    }
                }
                PatternSegment::Wildcard => {
                    // This should be handled at the higher level
                    return Err(nom::Err::Error(nom::error::Error::new(
                        segment,
                        nom::error::ErrorKind::Tag,
                    )));
                }
                PatternSegment::Placeholder => {
                    return Ok(vec![]); // Return empty vec for placeholders
                }
            }
        }

        // Handle compound segments by applying the pattern sequence to the segment
        let result = self.apply_pattern_sequence(segment, patterns)?;
        Ok(result)
    }

    fn apply_pattern_sequence(
        &self,
        input: &'a str,
        patterns: &[PatternSegment<'a>],
    ) -> Result<Vec<ExtractedSegment<'a>>, nom::Err<nom::error::Error<&'a str>>> {
        let mut results = Vec::new();
        let mut remaining = input;

        for (i, pattern) in patterns.iter().enumerate() {
            match pattern {
                PatternSegment::Named(name) => {
                    // Check if next pattern is also Named - that's an error
                    if let Some(PatternSegment::Named(_)) = patterns.get(i + 1) {
                        return Err(nom::Err::Error(nom::error::Error::new(
                            input,
                            nom::error::ErrorKind::Tag,
                        )));
                    }

                    // Only literals provide clear boundaries
                    if let Some(PatternSegment::Literal(literal)) = patterns.get(i + 1) {
                        let (new_remaining, value) = take_until(*literal)(remaining)?;
                        results.push(ExtractedSegment::Named(name, value));
                        remaining = new_remaining;
                    } else {
                        // No literal following, take everything remaining
                        let (new_remaining, value) = rest(remaining)?;
                        results.push(ExtractedSegment::Named(name, value));
                        remaining = new_remaining;
                    }
                }
                PatternSegment::Literal(literal) => {
                    let (new_remaining, matched) = tag(*literal)(remaining)?;
                    results.push(ExtractedSegment::Literal(literal, matched));
                    remaining = new_remaining;
                }
                _ => {} // Skip wildcards/placeholders in compound segments
            }
        }

        if !remaining.is_empty() {
            return Err(nom::Err::Error(nom::error::Error::new(
                input,
                nom::error::ErrorKind::Tag,
            )));
        }

        Ok(results)
    }
}

fn parse_placeholder(input: &str) -> IResult<&str, PatternSegment> {
    map(delimited(char('{'), char('_'), char('}')), |_| {
        PatternSegment::Placeholder
    })(input)
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
    alt((
        parse_placeholder,
        parse_named,
        parse_wildcard,
        parse_literal,
    ))(input)
}

// Parse the full pattern
fn parse_pattern(input: &str) -> IResult<&str, Vec<Vec<PatternSegment>>> {
    separated_list1(char('/'), parse_compound_segment)(input)
}

// Parse a single path segment that may contain multiple pattern segments
fn parse_compound_segment(input: &str) -> IResult<&str, Vec<PatternSegment>> {
    many1(parse_segment)(input)
}

impl<'a> PathPattern<'a> {
    pub fn new_with_nom(pattern: &'a str) -> Result<Self, anyhow::Error> {
        let cleaned_pattern = pattern.trim_start_matches('/');

        match parse_pattern(cleaned_pattern) {
            Ok((remaining, compound_segments)) => {
                if !remaining.is_empty() {
                    return Err(anyhow::anyhow!(
                        "Failed to parse entire pattern: '{}'",
                        remaining
                    ));
                }
                // Flatten the compound segments into a single Vec<PatternSegment>
                let segments = compound_segments.iter().flatten().cloned().collect();
                Ok(PathPattern {
                    pattern,
                    segments,
                    segment_groups: compound_segments,
                })
            }
            Err(e) => Err(anyhow::anyhow!("Pattern parsing failed: {}", e)),
        }
    }
}

pub fn extracted_segments_to_value(segments: &[ExtractedSegment]) -> Value {
    let mut result = json!({});

    for segment in segments {
        match segment {
            ExtractedSegment::Named(name, value) => {
                result[name] = json!(value);
            }
            ExtractedSegment::Literal(expected, matched) => {
                result[expected] = json!(matched);
            }
            ExtractedSegment::Wildcard(path_segments) => {
                result["wildcard"] = json!(path_segments);
            }
        }
    }

    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_path_pattern_simple() {
        let pattern = PathPattern::new("{environment}/{schema_table}").unwrap();

        assert_eq!(pattern.segments.len(), 2);
        assert!(matches!(pattern.segments[0], PatternSegment::Named(_)));
        assert!(matches!(pattern.segments[1], PatternSegment::Named(_)));
    }

    #[test]
    fn test_path_pattern_with_wildcard() {
        let pattern = PathPattern::new("{environment}/{schema_table}/*").unwrap();

        assert_eq!(pattern.segments.len(), 3);
        assert!(matches!(pattern.segments[2], PatternSegment::Wildcard));
    }

    #[test]
    fn test_path_pattern_with_literal() {
        let pattern = PathPattern::new("{environment}/data/{schema_table}").unwrap();

        assert_eq!(pattern.segments.len(), 3);
        if let PatternSegment::Literal(literal) = pattern.segments[1] {
            assert_eq!(literal, "data");
        } else {
            panic!("Expected literal segment");
        }
    }

    #[test]
    fn test_parse_path_basic() {
        let pattern = PathPattern::new("{environment}/{schema_table}").unwrap();
        let path = "/prod/schema.table/file.parquet";

        let extracted = pattern.parse_path(path).unwrap();

        assert_eq!(extracted.len(), 2);
        match extracted[0] {
            ExtractedSegment::Named(name, value) => {
                assert_eq!(name, "environment");
                assert_eq!(value, "prod");
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
        let pattern = PathPattern::new("{environment}/{schema_table}/{load_type}/*").unwrap();
        let path = "/prod/schema.table/delta/2025/06/25/file.snappy.parquet";

        let extracted = pattern.parse_path(path).unwrap();

        assert_eq!(extracted.len(), 4);
        match extracted[0] {
            ExtractedSegment::Named(name, value) => {
                assert_eq!(name, "environment");
                assert_eq!(value, "prod");
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
        let pattern = PathPattern::new("{environment}/data/{schema_table}").unwrap();
        let path = "/prod/data/schema.table/file.parquet";

        let extracted = pattern.parse_path(path).unwrap();

        assert_eq!(extracted.len(), 3);
        match extracted[1] {
            ExtractedSegment::Literal(expected, matched) => {
                assert_eq!(expected, "data");
                assert_eq!(matched, "data");
            }
            _ => panic!("Expected Literal segment"),
        }
    }

    #[test]
    fn test_extracted_segments_to_value_comprehensive() {
        let pattern = PathPattern::new("{environment}/data/{schema}.{table}/*").unwrap();
        let path = "/prod/data/public.users/2025/06/25/file.parquet";

        let extracted = pattern.parse_path(path).unwrap();
        let value = extracted_segments_to_value(&extracted);

        // Check that we have the expected structure
        assert_eq!(value["environment"], "prod");
        assert_eq!(value["data"], "data");
        assert_eq!(value["schema"], "public");
        assert_eq!(value["table"], "users");

        // Check wildcard array
        let wildcard = value["wildcard"].as_array().unwrap();
        assert_eq!(wildcard.len(), 4);
        assert_eq!(wildcard[0], "2025");
        assert_eq!(wildcard[1], "06");
        assert_eq!(wildcard[2], "25");
        assert_eq!(wildcard[3], "file.parquet");
    }

    #[test]
    fn test_extracted_segments_to_value_named_only() {
        let pattern = PathPattern::new("{environment}/{dataset}/{format}").unwrap();
        let path = "/prod/analytics/delta";

        let extracted = pattern.parse_path(path).unwrap();
        let value = extracted_segments_to_value(&extracted);

        // Should only have named segments
        assert_eq!(value["environment"], "prod");
        assert_eq!(value["dataset"], "analytics");
        assert_eq!(value["format"], "delta");

        // Should not have wildcard key
        assert!(value["wildcard"].is_null());
    }

    #[test]
    fn test_extracted_segments_to_value_with_multiple_literals() {
        let pattern = PathPattern::new("{environment}/raw/data/{table}/processed").unwrap();
        let path = "/prod/raw/data/transactions/processed";

        let extracted = pattern.parse_path(path).unwrap();
        let value = extracted_segments_to_value(&extracted);

        // Check named segments
        assert_eq!(value["environment"], "prod");
        assert_eq!(value["table"], "transactions");

        // Check literal segments
        assert_eq!(value["raw"], "raw");
        assert_eq!(value["data"], "data");
        assert_eq!(value["processed"], "processed");
    }

    #[test]
    fn test_placeholder_pattern() {
        let pattern = PathPattern::new("{environment}/data/{_}/{table}").unwrap();
        let path = "/prod/data/temp_folder/users";

        let extracted = pattern.parse_path(path).unwrap();
        let value = extracted_segments_to_value(&extracted);

        // Should only have named segments, placeholder should be ignored
        assert_eq!(value["environment"], "prod");
        assert_eq!(value["table"], "users");
        assert_eq!(value["data"], "data");

        // Should not have any key for the placeholder segment
        assert!(value.get("_").is_none());
        assert!(value.get("temp_folder").is_none());

        // Should have exactly 3 keys (environment, data, table)
        assert_eq!(value.as_object().unwrap().len(), 3);
    }
}
