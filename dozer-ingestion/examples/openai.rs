//  https://github.com/dongri/openai-api-rs
use dozer_types::openai_api_rs::v1::api::Client;
use dozer_types::openai_api_rs::v1::completion::{self, CompletionRequest};
use dozer_types::openai_api_rs::v1::error::APIError;
use dozer_types::serde::Deserialize;

const OPENAI_API_KEY: &str = "TEST";

#[derive(Debug, Deserialize)]
#[serde(crate = "dozer_types::serde")]
struct ModelsResponse {
    pub data: Vec<Model>,
}

#[derive(Debug, Deserialize)]
#[serde(crate = "dozer_types::serde")]
struct Model {
    pub id: String,
    pub object: String,
}

async fn _models(client: Client) -> Result<ModelsResponse, APIError> {
    let res = client.get("/models").await?;

    let r = res.json::<ModelsResponse>().await;
    match r {
        Ok(r) => Ok(r),
        Err(e) => Err(APIError {
            message: e.to_string(),
        }),
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // let client = Client::new(env::var("OPENAI_API_KEY").unwrap().to_string());
    let client = Client::new(OPENAI_API_KEY.to_string());

    let dav_msg = r#"# actor( actor_id, first_name, last_name, last_update)
         # film (film_id, title, description, release_year, language_id, original_language_id, rental_duration, rental_rate, length, replacement_cost, rating, last_update, special_features, fulltext)
        # film_actor (actor_id, film_id, last_update)
        ### A query to find all pairs of actors who have acted in the same film.
      "#;
    let req = CompletionRequest {
        model: completion::GPT3_TEXT_DAVINCI_003.to_string(),
        prompt: Some(String::from(dav_msg)),
        suffix: None,
        max_tokens: Some(300),
        temperature: Some(0.9),
        top_p: Some(1.0),
        n: None,
        stream: None,
        logprobs: None,
        echo: None,
        stop: None,
        presence_penalty: Some(0.0),
        frequency_penalty: Some(0.0),
        best_of: None,
        logit_bias: None,
        user: None,
    };
    let result = client.completion(req).await?;
    println!("{:?}", result.choices[0].text);
    Ok(())
}
