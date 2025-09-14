# Best Practices for Updating a Vertex AI Search Schema

Updating the schema of a live data store is a powerful but sensitive operation. Following these best practices will help
you avoid common errors and ensure your schema is applied correctly.

## The "Fetch and Merge" Workflow

The fundamental principle is to **always modify the existing schema**, not create a new one from scratch. The correct
workflow is:

1.  **Fetch** the current schema from the data store.
2.  **Parse** it into a dictionary.
3.  **Modify** this dictionary in memory by applying your desired changes.
4.  **Update** the data store with this modified schema object.

This ensures that you preserve the field types and `keyPropertyMapping` values that were automatically detected during
data ingestion.

## Key Rules and Restrictions (The "Don'ts")

Violating these rules is the most common cause of `400 Bad Request` errors.

### 1. Do Not Annotate Complex Types

Annotations like `retrievable`, `searchable`, `indexable`, `completable`, and `dynamicFacetable` can **only** be applied
to primitive types (e.g., `string`, `number`, `boolean`).

- **DON'T** apply these annotations at the top level of a field whose `type` is `array` or `object`.
- **DO** apply them to the `items` sub-field if the type is `array`.

**Incorrect:**

```json
"genre": {
  "type": "array",
  "retrievable": true, // WRONG: Annotation on the array itself
  "items": { "type": "string" }
}
```

**Correct:**

```json
"genre": {
  "type": "array",
  "items": {
    "type": "string",
    "retrievable": true // CORRECT: Annotation on the primitive item
  }
}
```

### 2. Do Not Modify `keyPropertyMapping` Fields

Certain fields are identified by the system as "Key Properties" because they have a special `keyPropertyMapping` (e.g.,
`"keyPropertyMapping": "title"`). These fields have strict rules.

- **DON'T** add or remove annotations from these fields. Only update the values of annotations that are already present.
- **DON'T** try to make them `searchable` or `indexable` if the API forbids it.

The safest approach is to **leave the annotations for these fields completely untouched**, or to explicitly remove
`searchable` and `indexable` from them before sending the update request.

**Restricted `keyPropertyMapping` values include:**

- `title`
- `uri`
- `categories`
- `media_available_time`
- `media_expire_time`
- `duration`
- `language_code`
- `persons`
- `filter_tags`

### 3. Do Not Use Dots (`.`) in Field Names

- **DON'T** define fields with dots in your schema (e.g., `"extended.content_category"`).
- **DO** use underscores (`_`) as a separator (e.g., `"extended_content_category"`). The application should normalize
  these names automatically before sending the update request.

### 4. Do Not Send an Incomplete Schema

- **DON'T** build a schema from scratch that only contains the fields you want to change.
- **DO** always start with the full schema you fetched from the server and modify it. The `update_schema` API expects
  the complete definition of all fields.

By following these rules, you can reliably and safely manage your data store's schema.
