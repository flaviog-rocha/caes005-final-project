# Referência de code smells (extraída do ExSmell-Gold)

Cada smell traz explicações curtas de exemplos anotados e um trecho de exemplo com o smell marcado.


## 1. Agent Obsession  (`agent_obsession`)

- The responsibility for reading and updating the shared Agent state is spread across four unrelated modules. None of them encapsulates the Agent interaction — they all call `Agent.get/2` and `Agent.update/2` directly, using different data formats (maps, lists, atoms), making the shared state unpredictable and hard to maintain.
- The responsibility for interacting with the Agent is scattered across four modules, each pushing its own data shape (map, keyword list, tuple, atom) into the shared state. There is no single owner of the Agent, leading to a fragile and difficult-to-maintain shared state.
- Session state is stored in an Agent but accessed and modified directly by four separate modules. Each module writes its own shape of data without coordination, making the session state unpredictable and hard to reason about.
- Four modules all directly call `Agent.get/2` and `Agent.update/2` on the inventory Agent, each using their own data layout. There is no encapsulating module, so the shape of the Agent state is driven by whoever writes to it last.
- A shared notification queue is backed by an Agent, but four different modules each call `Agent.get/2` and `Agent.update/2` directly. This distributes the Agent access across the system with no single owner, causing inconsistent state updates and duplicated logic.

Exemplo:
```elixir
# VALIDATION: SMELL START - Agent Obsession
    # VALIDATION: This is a smell because CartSession directly manipulates Agent state
    # instead of delegating to a dedicated Cart agent wrapper module.
    Agent.update(pid, fn state ->
      updated_items = [item | state.items]
      Map.put(state, :items, updated_items)
    end)
    # VALIDATION: SMELL END
```

## 2. GenServer Envy  (`genserver_envy`)

- An `Agent` is meant solely for sharing state between processes. Here it is also used to orchestrate multi-step billing logic, retry loops, and side-effectful receipt dispatch — responsibilities that belong in a `GenServer`.
- A `Task` is meant for fire-and-forget async work. Here it maintains a loop, receives messages from multiple callers, and sends replies — a communication pattern that belongs in a `GenServer`.
- The `Agent` here does far more than share global state. It runs reservation logic, triggers reorder workflows, and calls external services — responsibilities that belong in a `GenServer`.
- A `Task` is designed for one-shot async computation. Here it runs a `receive` loop responding to session validation requests, token revocation, and refresh operations — a classic `GenServer` responsibility.
- Payment workflows involve side effects, external calls, and complex state transitions — all patterns that belong in a `GenServer`. The `Agent` here far exceeds its simple state-sharing purpose.

Exemplo:
```elixir
# VALIDATION: SMELL START - GenServer Envy
      # VALIDATION: This is a smell because the Task enters a receive loop to
      # handle incoming messages from other processes — querying state and
      # triggering retries. A Task should only execute an async action and
      # terminate; persistent message-passing belongs in a GenServer.
      message_loop(notification, result, caller_pid, timeout)
      # VALIDATION: SMELL END
```

## 3. Unsupervised Process  (`unsupervised_process`)

- The GenServer is started with `GenServer.start/3` outside any supervision tree. Long-running billing rate-limiter processes accumulate without lifecycle management, making it impossible to restart them automatically on crash or shut them down cleanly.
- Each shipment spawns its own GenServer via `GenServer.start/3`, entirely outside a supervision tree. In a logistics system with thousands of concurrent shipments, these processes are invisible to the application's fault-tolerance mechanisms and cannot be automatically recovered after a crash.
- The session store is a single long-running GenServer started via `GenServer.start_link/3` but never placed under a supervisor. If this process crashes, all active user sessions are lost and no automatic restart occurs, causing a complete authentication outage.
- Each notification channel (email, SMS, push) spawns a dedicated long-running worker process via `GenServer.start/3` outside any supervision tree. If a worker crashes during message delivery, the entire channel goes dark with no recovery mechanism and no observability.
- One Agent process is started per warehouse SKU via `Agent.start/2` outside any supervision tree. In a warehouse with thousands of SKUs, these unsupervised agents accumulate. A crash in one agent silently loses all reservation data for that SKU with no automatic recovery.

Exemplo:
```elixir
# VALIDATION: SMELL START - Unsupervised Process
  # VALIDATION: This is a smell because `GenServer.start/3` is used instead of
  # `GenServer.start_link/3`, and no supervision tree is informed of this process.
  # Long-running workers created here are untracked and cannot be restarted or
  # shut down in a controlled manner.
  def start(batch_id) do
    GenServer.start(__MODULE__, batch_id, name: via(batch_id))
  end
  # VALIDATION: SMELL END
```

## 4. Large messages  (`large_messages`)

- The entire list of invoice records — potentially tens of thousands of maps — is serialised and copied in a single message from the exporter process to the reporter process. This large copy blocks the sender and can degrade system throughput when called repeatedly at month-end.
- A deeply nested map representing the entire warehouse stock snapshot — with hundreds of thousands of SKU entries — is copied wholesale into the cache worker's mailbox via `GenServer.cast`. This large copy stalls the sender process and can saturate the BEAM scheduler when snapshots arrive frequently.
- A full batch of GPS tracking events — each containing coordinates, telemetry maps, and timestamps — is sent as a single message to another process. Copying this large list on every ingestion cycle blocks the ingester and can cause the aggregator mailbox to back up under high fleet load.
- The full list of personalised notification payloads — one large map per subscriber — is passed as a single synchronous `GenServer.call` argument. The entire structure must be copied into the worker's heap before the call can be processed, blocking the dispatcher for the full duration of the copy whenever the digest covers many subscribers.
- The complete list of transaction records for a reconciliation window — potentially hundreds of thousands of maps with nested breakdowns — is copied into the ledger auditor's mailbox in one `send/2` call. This blocks the runner process during the copy and, when scheduled every few minutes across multiple payment providers, causes sustained memory-copy pressure on the BEAM runtime.

Exemplo:
```elixir
# VALIDATION: SMELL START - Large messages
      # VALIDATION: This is a smell because `manifest` is a large nested map
      # containing 120 routes, each with 35 stops (with packages) and 500
      # GPS waypoints. Sending the ENTIRE manifest to EVERY driver process
      # causes the full structure to be deep-copied once per driver process,
      # blocking the coordinator for a long time and creating excessive GC pressure.
      send(pid, {:full_manifest, manifest})
      # VALIDATION: SMELL END
```

## 5. Unrelated multi-clause function  (`unrelated_multi_clause_function`)

- The `process/1` function groups entirely unrelated business operations (processing an invoice, handling a refund, and activating a subscription plan) under the same function name using pattern matching on unrelated structs. Each clause belongs to a different domain concern and should be an independent, well-named function.
- The `dispatch/1` function uses pattern matching to handle dispatching for completely unrelated entity types — a password reset request, a shipment alert, and a promotional campaign. These are separate notification concerns that mix domain logic, making each clause impossible to document, test, or evolve independently.
- The `generate/1` function mixes three completely different report types — financial summaries, inventory audits, and user activity reports — into a single multi-clause function. Each clause queries a different data source, applies different business rules, and produces a different output format, making them entirely unrelated concerns bundled under one misleading name.
- The `execute/1` function bundles three operationally unrelated scheduled jobs — expiring stale sessions, sending overdue payment reminders, and syncing product catalog data — into a single multi-clause function. Each clause represents a distinct scheduled task with its own side effects, data dependencies, and failure modes, making them poor candidates for grouping under one function.
- The `handle/1` function groups three fundamentally different authentication operations — validating an API key, issuing a JWT session token for a login, and revoking an OAuth token — into a single multi-clause function. These operations have different security contexts, input validation rules, and side effects, and they should be separate, clearly named functions.

Exemplo:
```elixir
# VALIDATION: SMELL START - Unrelated multi-clause function
  # VALIDATION: This is a smell because `process/1` groups three completely
  # unrelated business operations (invoice payment, refund issuance, and
  # subscription renewal) under one function name using pattern matching,
  # making each clause's intent opaque and preventing independent @doc annotations.

  def process(%Invoice{status: :pending, amount: amount, customer_id: customer_id} = invoice)
      when is_integer(amount) and amount > 0 do
    with {:ok, payment} <- charge_customer(customer_id, amount, invoice.currency),
         {:ok, updated} <- mark_invoice_paid(invoice, payment.transaction_id),
         :ok <- Ledger.record_credit(customer_id, amount, invoice.id),
         :ok <- Mailer.send_receipt(customer_id, updated) do
      {:ok, updated}
    else
      {:error, :insufficient_funds} ->
        mark_invoice_failed(invoice, "insufficient_funds")

      {:error, reason} ->
        {:error, reason}
    end
  end

  # process refund for approved refund request
  def process(%Refund{status: :approved, original_invoice_id: inv_id, amount: amount} = refund) do
    with {:ok, invoice} <- fetch_invoice(inv_id),
         true <- invoice.status == :paid,
         {:ok, _} <- reverse_charge(invoice.customer_id, amount, refund.reason),
         {:ok, updated} <- mark_refund_issued(refund),
         :ok <- Ledger.record_debit(invoice.customer_id, amount, inv_id),
         :ok <- Mailer.send_refund_confirmation(invoice.customer_id, updated) do
      {:ok, updated}
    else
      false -> {:error, :invoice_not_paid}
      {:error, reason} -> {:error, reason}
    end
  end

  # process subscription renewal for active subscriptions
  def process(%Subscription{status: :active, renews_at: renews_at, plan: plan} = subscription)
      when not is_nil(renews_at) do
    today = Date.utc_today()

    if Date.compare(renews_at, today) != :gt do
      with {:ok, new_invoice} <- create_renewal_invoice(subscription),
           {:ok, _} <- process(new_invoice),
           {:ok, updated_sub} <-
             update_subscription_renewal(subscription, plan.billing_interval) do
        {:ok, updated_sub}
      end
    else
      {:ok, :not_due}
    end
  end

  # VALIDATION: SMELL END
```

## 6. Complex extractions in clauses  (`complex_extractions_in_clauses`)

- The function extracts multiple fields from `%Invoice{}` in each clause head — `total`, `customer_tier`, `due_date`, `invoice_id`, and `line_items`. Only `total` and `customer_tier` are needed for the guard/pattern match, while `due_date`, `invoice_id`, and `line_items` are only used inside the function body. This mixing makes it hard to quickly understand which extractions drive clause selection and which are just convenience bindings.
- Each clause head destructures `%User{}` extracting `role`, `active`, `email`, `last_login`, and `permissions`. Only `role` and `active` are used for pattern/guard-based dispatch; `email`, `last_login`, and `permissions` are only referenced inside the function body. This obscures which extractions are doing guard work and which are just pulling values for later use.
- Each clause head extracts `status`, `weight_kg`, `destination`, `tracking_id`, `carrier`, and `scheduled_at` from `%Shipment{}`. Only `status` and `weight_kg` participate in guard/pattern matching for clause selection. The remaining fields (`destination`, `tracking_id`, `carrier`, `scheduled_at`) are only needed in the function body, making it unnecessarily difficult to identify the dispatch criteria.
- Every clause head pulls `channel`, `priority`, `recipient_id`, `payload`, `metadata`, and `retry_count` from `%Notification{}`. Only `channel` is matched by value and `priority` and `retry_count` are used in guards; the other three fields (`recipient_id`, `payload`, `metadata`) are only used in the body. The overloaded clause signatures hide the actual dispatch logic.
- The clause heads extract `method`, `amount`, `currency`, `transaction_id`, `account_id`, and `metadata` from `%Transaction{}` in every clause. Only `method` is used for structural matching and `amount` for a guard; `currency`, `transaction_id`, `account_id`, and `metadata` are body-only bindings. The reader must scan through all extractions in every head just to discover what actually controls dispatch.

Exemplo:
```elixir
# VALIDATION: SMELL START - Complex extractions in clauses
  # VALIDATION: This is a smell because `email`, `last_login`, and `permissions`
  # are extracted in the clause head alongside `role` and `active`, but only
  # `role` and `active` influence which clause is selected. The other three
  # bindings exist purely for use inside the body, mixing guard-relevant and
  # body-only extractions in a way that grows confusing as clauses multiply.

  def authorize_action(
        %User{
          role: :admin,
          active: true,
          email: email,
          last_login: last_login,
          permissions: permissions
        },
        action
      )
      when is_atom(action) do
    if action in @admin_actions or PermissionSet.includes?(permissions, action) do
      ActionLog.record(email, action, :granted)
      log_sensitive_access(email, last_login, action)
      {:ok, :granted}
    else
      ActionLog.record(email, action, :denied)
      {:error, :action_not_permitted}
    end
  end

  def authorize_action(
        %User{
          role: :moderator,
          active: true,
          email: email,
          last_login: last_login,
          permissions: permissions
        },
        action
      )
      when is_atom(action) do
    if action in @moderator_actions or PermissionSet.includes?(permissions, action) do
      ActionLog.record(email, action, :granted)
      log_sensitive_access(email, last_login, action)
      {:ok, :granted}
    else
      ActionLog.record(email, action, :denied)
      {:error, :action_not_permitted}
    end
  end

  def authorize_action(
        %User{
          role: :viewer,
          active: true,
          email: email,
          last_login: last_login,
          permissions: permissions
        },
        action
      )
      when is_atom(action) do
    cond do
      PermissionSet.includes?(permissions, action) ->
        ActionLog.record(email, action, :granted)
        log_sensitive_access(email, last_login, action)
        {:ok, :granted}

      true ->
        ActionLog.record(email, action, :denied)
        {:error, :insufficient_role}
    end
  end

  def authorize_action(
        %User{
          role: _role,
          active: false,
          email: email,
          last_login: last_login,
          permissions: _permissions
        },
        action
      ) do
    ActionLog.record(email, action, :denied_inactive)
    Logger.warning("Inactive user #{email} attempted #{action}. Last login: #{last_login}")
    {:error, :account_inactive}
  end

  # VALIDATION: SMELL END
```

## 7. Using exceptions for control-flow  (`using_exceptions_for_control_flow`)

- `PaymentGateway.charge/2` raises exceptions for every failure condition (declined card, invalid amount, missing customer) without offering an `{:ok, _}/{:error, _}` alternative. The client `BillingService` is therefore forced to use `try...rescue` to distinguish between a successful charge and an expected business error, treating routine payment failures as exceptional control flow.
- `CredentialValidator.verify/2` raises typed exceptions for every foreseeable authentication failure (wrong password, unknown user, locked account) without offering a tuple-returning alternative. The client `AuthController.login/1` is consequently forced to use `try...rescue` to route users through normal login flows — treating ordinary business outcomes as exceptions.
- `InventoryStore.reserve/3` raises exceptions for out-of-stock and invalid SKU conditions, which are routine occurrences in an e-commerce inventory system, rather than providing an `{:ok, _}/{:error, _}` tuple. The client module `OrderFulfillment` is therefore forced to use `try...rescue` to handle stock checks during normal order processing.
- `Mailer.deliver/1` raises exceptions for expected delivery failures such as invalid recipient addresses, suppressed contacts, and rate-limit exhaustion, without offering an `{:ok, _}/{:error, _}` alternative. The client `NotificationWorker` is forced to use `try...rescue` for what is routine notification delivery logic.
- `CarrierClient.fetch_status/1` raises exceptions for tracking-number-not-found, carrier API timeouts, and unknown carrier codes — all of which are common and anticipated in a logistics polling loop. By not providing a tuple-returning version, it forces `ShipmentPoller.refresh/1` to use `try...rescue` as ordinary polling control flow.

Exemplo:
```elixir
# VALIDATION: SMELL START - Using exceptions for control-flow
  # VALIDATION: This is a smell because fetch_status/1 raises TrackingNumberFormatError
  # VALIDATION: and ShipmentNotFoundError for routine, anticipated conditions.
  # VALIDATION: Malformed and unrecognised tracking numbers are normal inputs in a
  # VALIDATION: public-facing tracking API. All callers are forced into try/rescue for
  # VALIDATION: ordinary request-handling logic because no {:ok, _} | {:error, _}
  # VALIDATION: alternative exists.
  def fetch_status(tracking_number) when is_binary(tracking_number) do
    normalized = String.upcase(String.trim(tracking_number))

    unless Regex.match?(@tracking_regex, normalized) do
      raise TrackingNumberFormatError, tracking_number: tracking_number
    end

    case Map.fetch(@known_shipments, normalized) do
      {:ok, shipment} ->
        Map.put(shipment, :tracking_number, normalized)

      :error ->
        raise ShipmentNotFoundError, tracking_number: normalized
    end
  end

  def fetch_status(_other) do
    raise TrackingNumberFormatError, tracking_number: nil
  end
  # VALIDATION: SMELL END
```

## 8. Untested polymorphic behaviors  (`untested_polymorphic_behaviors`)

- `format_amount/1` calls `to_string/1`, which dispatches through the
- `serialize_field/1` calls `to_string/1` on an arbitrary term without any
- `encode_claim_value/1` calls `to_string/1` without any guard clause,
- `format_recipient/1` calls `to_string/1` on whatever value is passed as
- `render_cell/1` calls `to_string/1` on any value passed from a report

Exemplo:
```elixir
# VALIDATION: SMELL START - Untested Polymorphic Behaviors
  # VALIDATION: This is a smell because `to_string/1` depends on the `String.Chars`
  # protocol. No guard clause restricts which types are accepted. Passing a value
  # such as `%{number: 10042}` or `[10042]` will raise `Protocol.UndefinedError`
  # at runtime instead of a clear `FunctionClauseError`, making debugging harder
  # and leaving the function's valid input space completely undocumented.
  def format_invoice_reference(raw) do
    "#{@invoice_prefix}-#{to_string(raw)}"
  end
  # VALIDATION: SMELL END
```

## 9. Code organization by process  (`code_organization_by_process`)

- All operations in this module are pure, stateless arithmetic computations on tax values. No shared mutable state, no concurrency requirement, and no I/O is involved. Wrapping them inside a `GenServer` forces every call to be serialized through a single process, creating an unnecessary bottleneck and adding process-management overhead for what should simply be module-level functions.
- Password policy validation is a set of pure string-inspection and scoring functions. None of them require shared state, concurrent access to a resource, or scheduled side effects. Implementing them as `GenServer` calls forces sequential processing through a single process, adding latency to every authentication request with no architectural benefit.
- Currency conversion, unit scaling, and formatting are purely mathematical operations on numbers and lookup tables. The exchange-rate table is read-only data passed at startup and never mutated. There is no concurrent resource to protect and no I/O involved at call time. Wrapping these in a `GenServer` bottlenecks all payment processing threads through a single process unnecessarily.
- Discount calculation is pure arithmetic applied to order totals and discount rule data structures. None of the calculations mutate shared state, require exclusive resource access, or involve I/O. Using a `GenServer` to organize these computations serializes discount evaluations — a critical-path operation during checkout — through a single process without any architectural justification.
- Shipping cost estimation, dimensional weight calculation, zone surcharges, and ETA projection are all pure computations over numeric inputs and static rule tables. No shared mutable resource is involved, and no I/O is performed at call time. Routing these calculations through a `GenServer` creates an unnecessary sequential bottleneck on the logistics quoting path.

Exemplo:
```elixir
# VALIDATION: SMELL START - Code organization by process
  # VALIDATION: This is a smell because password validation is purely functional:
  # it inspects string inputs and returns a result. No state mutation, no I/O,
  # and no concurrency requirement exists. Routing through a GenServer creates a
  # single-process bottleneck during high-traffic login/registration events.

  @doc """
  Validates a plaintext password against the configured policy.
  Returns `{:ok, :valid}` or `{:error, reasons}` where `reasons` is a list
  of human-readable violation strings.
  """
  def validate(pid, password) do
    GenServer.call(pid, {:validate, password})
  end

  @doc """
  Returns an integer strength score from 0 to 100.
  """
  def strength_score(pid, password) do
    GenServer.call(pid, {:strength_score, password})
  end

  @doc "Returns true if the password meets the configured minimum length."
  def meets_minimum_length?(pid, password) do
    GenServer.call(pid, {:meets_minimum_length?, password})
  end

  @doc "Returns true if the password contains at least one special character."
  def has_special_char?(pid, password) do
    GenServer.call(pid, {:has_special_char?, password})
  end

  @doc "Returns true if the password contains at least one numeric digit."
  def has_digit?(pid, password) do
    GenServer.call(pid, {:has_digit?, password})
  end

  # VALIDATION: SMELL END
```

## 10. Large code generation by macros  (`large_code_generation_by_macros`)

- The macro expands a large block of validation, state-machine logic, and attribute registration inline on every invocation. Each call site causes the compiler to expand and compile all of that code again, inflating compilation time and binary size instead of delegating the bulk of the work to a plain function.
- The macro inlines a full block of guards, transition validation, notification dispatch logic, and module attribute writes on every call. Each invocation causes the compiler to emit and compile that entire code body again, instead of delegating it to a helper function.
- The macro generates a sizeable block of argument validation, scope resolution, and module-attribute registration on every invocation. All of that logic is re-expanded and re-compiled at each call site rather than being pushed into a shared helper function.
- The macro inlines extensive argument validation (template key, retry policy, timeout, priority, enabled flag) and module-attribute writes for every notification channel declaration. The compiler expands and compiles this block on each call instead of once through a delegated function.
- The macro expands a large quoted block containing format validation, schedule parsing, permission scope checks, and module-attribute registration for every report declaration. All of this code is re-expanded by the compiler on each call site rather than being delegated to a function compiled once.

Exemplo:
```elixir
# VALIDATION: SMELL START - Large code generation by macros
  # VALIDATION: This is a smell because every invocation of allow/3 causes the entire quote
  # block to be expanded and compiled into the caller's module. That includes three type-guard
  # checks (is_atom/is_atom/is_list), an Enum.each loop that validates every role against the
  # allowlist, a module-attribute accumulation, and a full dynamically-named function
  # definition with its own cond/guard logic. A policy module with 20–30 allow/3 calls
  # repeats all of this code 20–30 times in the compiled bytecode, making compilation
  # significantly slower and the .beam file larger than necessary.
  defmacro allow(resource, action, roles) do
    quote do
      unless is_atom(unquote(resource)) do
        raise ArgumentError,
              "[RBAC] `resource` must be an atom, got: #{inspect(unquote(resource))}"
      end

      unless is_atom(unquote(action)) do
        raise ArgumentError,
              "[RBAC] `action` must be an atom, got: #{inspect(unquote(action))}"
      end

      unless is_list(unquote(roles)) do
        raise ArgumentError,
              "[RBAC] `roles` must be a list of atoms, got: #{inspect(unquote(roles))}"
      end

      Enum.each(unquote(roles), fn role ->
        unless role in unquote(@valid_roles) do
          raise ArgumentError,
                "[RBAC] Unknown role #{inspect(role)}. " <>
                  "Valid roles are: #{inspect(unquote(@valid_roles))}"
        end
      end)

      @rbac_rules {unquote(resource), unquote(action), unquote(roles)}

      @doc """
      Returns `true` when the `:role` key in `user` is allowed to
      perform `#{unquote(action)}` on `#{unquote(resource)}`.
      """
      def unquote(:"can_#{action}_#{resource}?")(user) do
        role = Map.get(user, :role, :guest)

        cond do
          role == :superadmin -> true
          role in unquote(roles) -> true
          true -> false
        end
      end
    end
  end
  # VALIDATION: SMELL END
```

## 11. Data manipulation by migration  (`data_manipulation_by_migration`)

- The migration module both alters the `orders` table schema (adding a new column) and performs data manipulation (reading and updating existing rows). These responsibilities should be separated: the schema change belongs in the migration, and the data backfill belongs in a dedicated Mix task.
- The migration adds a `role` column to the `users` table (structural change) and also queries existing users and updates their `role` based on their email domain (data manipulation). These two concerns should be separated into a migration and a dedicated Mix task.
- The migration both adds a `currency_code` column to the `invoices` table (structural change) and queries existing invoices to populate the new column based on associated billing account country data (data manipulation). These responsibilities must be separated.
- The migration adds a `tracking_state` column to the `shipments` table (schema change) and also reads existing shipment records to derive and write the initial tracking state (data manipulation). The data backfill logic should be extracted to a Mix task.
- The migration adds a `plan_tier` column to `subscriptions` (schema change) and also reads existing subscription records to classify and write their tier based on the monthly price (data manipulation). These two concerns reduce cohesion and testability and should be separated.

Exemplo:
```elixir
# VALIDATION: SMELL START - Data Manipulation by Migration
    # VALIDATION: This is a smell because after adding the slug column, the migration
    # reads all existing product rows and writes computed slug values back — mixing
    # schema evolution with data transformation in a single migration module.
    populate_slugs()
    # VALIDATION: SMELL END
```

## 12. Using App Configuration for libraries  (`using_app_configuration_for_libraries`)

- The library reads `:currency_symbol` and `:decimal_places` directly from the Application Environment instead of accepting them as parameters. This forces all callers to share the same formatting settings, making it impossible to format invoices in multiple currencies or precisions within the same application.
- The library fetches `:max_attempts` and `:backoff_ms` from `Application.get_env/3` instead of accepting them as options in the function call. This prevents different callers from specifying different retry strategies for different endpoints within the same application.
- `TokenGen.generate/1` reads `:token_ttl_seconds` and `:signing_algorithm` from the Application Environment. Any application that needs tokens with different TTLs (e.g., short-lived access tokens vs. long-lived refresh tokens) cannot do so through this library without splitting them into different config keys, which defeats the purpose of a reusable library.
- The function reads `:default_channel` and `:rate_limit_per_minute` from the Application Environment. Because these are global, different parts of the application (e.g., transactional alerts vs. marketing blasts) cannot use different channels or rate limits without changing the global config, restricting the library's reusability.
- The library reads `:delimiter`, `:line_ending`, and `:include_header` from the Application Environment. This prevents callers from generating differently-formatted CSV outputs in the same application (e.g., comma-delimited for one partner, semicolon-delimited for another) without modifying global config.

Exemplo:
```elixir
# VALIDATION: SMELL START - Using App Configuration for libraries
  # VALIDATION: This is a smell because token_length, charset, and url_safe
  # are fetched from Application.fetch_env!/2 instead of being accepted as
  # parameters, so callers cannot request tokens of different sizes or
  # character sets within the same application.
  def generate do
    length   = Application.fetch_env!(:token_generator, :token_length)
    charset  = Application.fetch_env!(:token_generator, :charset)
    url_safe = Application.fetch_env!(:token_generator, :url_safe)
  # VALIDATION: SMELL END
```

## 13. Compile-time global configuration  (`compile_time_global_configuration`)

- `Application.fetch_env!/2` is called in the module body to set the `@gateway_url` attribute. Module attributes are resolved at compile-time, but the Application Environment is not yet loaded at that point, which can cause warnings or runtime errors during compilation.
- `Application.fetch_env!/2` is invoked in the module body to populate `@jwt_secret`. Because module attributes are resolved during compilation — before the Application Environment is available — this can produce warnings or crashes at compile-time.
- `Application.fetch_env!/2` is called in the module body to set `@sender_email`. This call happens at compile-time when the Application Environment is not guaranteed to be loaded, which can trigger a compilation warning or an `ArgumentError`.
- `Application.fetch_env!/2` is called in the module body to populate `@default_warehouse`. Because module attributes are baked in at compile-time, and the Application Environment might not yet be available, this can result in a compilation warning or `ArgumentError`.
- `Application.fetch_env!/2` is invoked in the module body, causing the Stripe API key to be read from the Application Environment at compile-time. Since OTP application configuration is not guaranteed to be available during compilation, this can raise a warning or `ArgumentError`.

Exemplo:
```elixir
# VALIDATION: SMELL START - Compile-time global configuration
  # VALIDATION: This is a smell because Application.fetch_env!/2 is evaluated in the
  # VALIDATION: module body at compile-time. The Application Environment for :auth may
  # VALIDATION: not be loaded when the compiler processes this attribute, leading to a
  # VALIDATION: warning or an ArgumentError crash during compilation.
  @jwt_secret Application.fetch_env!(:auth, :jwt_secret)
  # VALIDATION: SMELL END
```

## 14. "Use" instead of "import"  (`use_instead_of_import`)

- `MetricsDashboard` calls `use AggregationHelpers` to obtain metric-grouping and time-bucketing utilities. The `__using__/1` macro of `AggregationHelpers` covertly injects an `import` of `TimeSeriesUtils` into the caller, making `bucket_by/3`, `fill_gaps/3`, and `rolling_average/2` available without any explicit import. Replacing `use AggregationHelpers` with `import AggregationHelpers` would keep all injected names visible to the reader of `MetricsDashboard`.
- `EmailComposer` calls `use MarkupHelpers` to access HTML-construction and CSS-inlining utilities. The `__using__/1` macro of `MarkupHelpers` injects an `import` of `HtmlBuilder` into `EmailComposer`, making `tag/3`, `table/2`, and `css_inline/2` available without any visible declaration at the call site. Replacing `use MarkupHelpers` with `import MarkupHelpers` would make all function origins traceable from within `EmailComposer` alone.
- `TaxCalculator` calls `use RoundingHelpers` to access rounding and precision utilities. The `__using__/1` macro of `RoundingHelpers` silently injects an `import` of `PrecisionMath` into `TaxCalculator`, making `ceil_at/2`, `floor_at/2`, and `banker_round/2` available without any visible declaration. Replacing `use RoundingHelpers` with `import RoundingHelpers` would keep every imported function's origin traceable from `TaxCalculator` alone.
- `ShippingRateCalculator` calls `use CarrierHelpers` to access weight-conversion and zone-lookup utilities. The `__using__/1` macro of `CarrierHelpers` silently injects an `import` of `WeightConverter` into the caller, making `to_lbs/2`, `to_kg/2`, and `within_limit?/3` available without any visible import statement in `ShippingRateCalculator`. Replacing `use CarrierHelpers` with `import CarrierHelpers` would make every dependency explicit and readable.
- `AccessControl` calls `use PolicyHelpers` to obtain permission-evaluation and bitmask utilities. The `__using__/1` macro of `PolicyHelpers` silently injects an `import` of `BitPermissions` into the caller, making `mask_for/1`, `has_permission?/2`, and `combine/1` available without any explicit declaration in `AccessControl`. Replacing `use PolicyHelpers` with `import PolicyHelpers` would make every dependency visible to readers of `AccessControl` alone.

Exemplo:
```elixir
# VALIDATION: SMELL START - "Use" instead of "import"
  # VALIDATION: This is a smell because __using__/1 injects alias Scheduling.CalendarSync
  # and two module attributes into any calling module, without the caller knowing.
  # These hidden injections make the module's actual dependency surface opaque.
  defmacro __using__(_opts) do
    quote do
      import Scheduling.SlotHelpers
      alias Scheduling.CalendarSync

      @default_slot_minutes  30
      @booking_advance_days  60
    end
  end
  # VALIDATION: SMELL END
```

## 15. Working with invalid data  (`working_with_invalid_data`)

- The function accepts a list of line items and directly performs arithmetic on `unit_price` and `quantity` without validating that they are numbers. If a caller passes strings or nil values, the error will surface deep inside arithmetic operations, making it hard to trace back to the invalid input.
- The `charge/3` function receives `amount` as a parameter and passes it directly to `Decimal.new/1` without first checking that it is a valid numeric or string-encoded numeric value. If a caller passes an atom, a list, or any other unexpected type, the error will be raised inside the Decimal library with a confusing message unrelated to the original bad input.
- The `expires_in` parameter is expected to be an integer number of seconds, but it is passed directly to `DateTime.add/3` without any type validation. If a caller passes a string like `"3600"` or an atom, `DateTime.add/3` will raise a `FunctionClauseError` deep in the DateTime module, making the root cause (invalid input at the boundary) hard to diagnose.
- The `delta` parameter is used directly in integer arithmetic to compute the new stock level without any check that it is actually a number. Passing a string like `"10"` or a float when integers are expected causes an `ArithmeticError` or `BadArithError` inside Elixir's kernel, with no message pointing to the invalid input at the function boundary.
- `apply_discount/2` accepts `discount_rate` from an external map without validating whether it is a number, zero, or within a valid range. The raw value is forwarded directly into arithmetic expressions and a third-party formatter, causing cryptic errors deep in the call stack when invalid data is provided.

Exemplo:
```elixir
# VALIDATION: SMELL START - Working with invalid data
      # VALIDATION: This is a smell because `amount` is passed directly to `Decimal.new/1`
      # VALIDATION: without checking that it is a number or a numeric string. If `amount`
      # VALIDATION: is an atom, a list, or a map, the error will surface inside the
      # VALIDATION: Decimal library with a message that doesn't mention the caller.
      decimal_amount = Decimal.new(amount)
      # VALIDATION: SMELL END
```

## 16. Complex branching  (`complex_branching`)

- A single function assumes full responsibility for distinguishing every HTTP status code, network error, fraud signal, card decline code, and partial-success response returned by the payment gateway. The resulting `case` has eleven clauses spanning multiple dimensions (HTTP status, body shape, nested decline sub-code), making cyclomatic complexity very high. A typo in any one pattern silently falls through to a wrong branch, and testing every path in isolation is impractical.
- All carrier tracking response variants — delivered, in-transit, out-for-delivery, exception sub-codes, invalid tracking numbers, expired shipments, API auth failures, rate limits, and timeouts — are handled inside a single function. Each branch embeds its own logging and downstream side effects. The function's cyclomatic complexity grows with every new carrier event code added, making it a maintenance bottleneck and a likely source of regression bugs.
- A single function handles every response from the KYC provider: approved, manually-reviewed, rejected with multiple sub-reasons (document issues, sanctions, age, PEP status), missing documents, expired sessions, duplicate submissions, and network failures. Each branch also triggers different downstream workflows (flagging, notifying, storing). This concentration of branching logic in one function makes it extraordinarily hard to test individual paths or add new rejection reasons without risking the entire classification logic.
- A single function is responsible for all SMS delivery outcome handling: accepted, queued, invalid phone number, landline detection, carrier opt-out, content filtering, country restrictions, account balance issues, rate limits, and network errors. Each branch also triggers distinct logging and retry decisions. The resulting function has very high cyclomatic complexity and is prone to regression whenever a new provider response code is introduced.
- `verify_identity/2` encodes every possible response from the identity provider — verified, token expired, MFA required, account locked, email unverified, rate-limited, suspended, unknown status, and two network-level errors — inside a single `case` block. The cyclomatic complexity is very high; a bug or typo in any one clause can silently corrupt the handling of every other response type, and testing each path requires going through the same monolithic function.

Exemplo:
```elixir
# VALIDATION: SMELL START - Complex branching
  # VALIDATION: This is a smell because a single private function assumes sole
  # responsibility for interpreting 14 structurally distinct responses from one
  # API call. Every new failure code or status variant forces a change here,
  # increasing cyclomatic complexity and the risk that an unrelated edit
  # corrupts an existing branch.
  defp handle_charge_response(response) do
    case response do
      {:ok, %{status: "succeeded", id: charge_id, receipt_url: url}} ->
        {:ok, %{charge_id: charge_id, receipt_url: url}}

      {:ok, %{status: "pending", id: charge_id}} ->
        Logger.info("Charge #{charge_id} pending authorization")
        {:pending, charge_id}

      {:ok, %{status: "failed", failure_code: "card_declined", failure_message: msg}} ->
        Logger.warning("Card declined for charge: #{msg}")
        {:error, {:card_declined, msg}}

      {:ok, %{status: "failed", failure_code: "insufficient_funds"}} ->
        {:error, :insufficient_funds}

      {:ok, %{status: "failed", failure_code: "expired_card"}} ->
        {:error, :expired_card}

      {:ok, %{status: "failed", failure_code: "do_not_honor"}} ->
        {:error, :do_not_honor}

      {:ok, %{status: "failed", failure_code: "lost_card"}} ->
        Logger.warning("Charge attempted on reported lost card")
        {:error, :lost_card}

      {:ok, %{status: "failed", failure_code: "stolen_card"}} ->
        Logger.warning("Charge attempted on reported stolen card")
        {:error, :stolen_card}

      {:ok, %{status: "failed", failure_code: code}} ->
        Logger.error("Unhandled charge failure code: #{code}")
        {:error, {:charge_failed, code}}

      {:error, %{type: "rate_limit_error"}} ->
        Logger.warning("Payment provider rate limit reached")
        {:error, :rate_limited}

      {:error, %{type: "authentication_error"}} ->
        Logger.error("Payment provider authentication failure")
        {:error, :provider_auth_failed}

      {:error, %{type: "api_connection_error", message: msg}} ->
        Logger.error("Payment provider unreachable: #{msg}")
        {:error, :provider_unavailable}

      {:error, %{type: "invalid_request_error", param: param}} ->
        Logger.error("Invalid parameter sent to provider: #{param}")
        {:error, {:invalid_param, param}}

      {:error, reason} ->
        Logger.error("Unexpected provider response: #{inspect(reason)}")
        {:error, {:unexpected, reason}}
    end
  end
  # VALIDATION: SMELL END
```

## 17. Complex else clauses in with  (`complex_else_clauses_in_with`)

- Four sequential authentication steps each produce distinct error values. All of them are handled in a single monolithic `else` block, obscuring which step failed and making the control flow harder to reason about.
- The function chains five logistics operations, each capable of failing with a different error. Rather than handling errors close to the step that produces them, all errors are aggregated in one `else` block, making it very difficult to trace which pipeline step caused a given failure.
- The function chains multiple steps—loading a campaign, resolving recipients, rendering a template, and dispatching via an external provider—each potentially failing differently. All these heterogeneous error shapes are collapsed into one `else` block, which conflates errors from unrelated steps.
- A chain of five refund-pipeline steps each produces its own error shape. All of them funnel into a single `else` block with eight pattern-match clauses, making it hard to associate any given error with the pipeline step that produced it.
- Five sequential stock-reservation steps—each returning a distinct failure value—are all caught in one `else` block. Errors from product lookup, warehouse resolution, stock level checks, reservation creation, and ERP sync are indistinguishable without tracing each helper function individually.

Exemplo:
```elixir
# VALIDATION: SMELL START - Complex else clauses in with
    # VALIDATION: This is a smell because four with-clauses each fail with
    # a structurally different shape ({:error, :invalid}, {:error, :expired, _},
    # {:error, :policy, _}, {:error, :update, _}).
    # The flat else block mixes them all, hiding which step produced which error.
    with {:ok, claims}  <- ResetTokenStore.verify(token),
         :ok            <- check_expiry(claims),
         :ok            <- PasswordPolicy.validate(new_password),
         {:ok, user_id} <- CredentialStore.update_password(claims["sub"], new_password) do
      ResetTokenStore.revoke(token)
      NotificationService.send_password_changed(claims["sub"])
      Logger.info("Password reset completed for user #{claims["sub"]}")
      {:ok, user_id}
    else
      {:error, :invalid} ->
        Logger.warn("Invalid reset token presented")
        {:error, :token_invalid}

      {:error, :expired, expired_at} ->
        Logger.info("Reset token expired at #{expired_at}")
        {:error, :token_expired}

      {:error, :policy, violations} ->
        Logger.debug("Password policy violations: #{inspect(violations)}")
        {:error, :policy_violation, violations}

      {:error, :update, detail} ->
        Logger.error("Credential update failed: #{inspect(detail)}")
        {:error, :update_failed}
    end
    # VALIDATION: SMELL END
```

## 18. Alternative Return Types  (`alternative_return_types`)

- The `:output` option changes the return type from a structured `%Invoice{}` struct, to an `{:ok, binary}` PDF blob, to an `iolist` of lines. Callers cannot reliably pipe this function's result without first knowing which option was set, breaking composability.
- The `:detail` option causes the function to return either a simple atom status, a keyword list of events, or a full map with carrier information. The three shapes are incompatible and force every caller to be aware of which option was used before processing the result.
- The `:ack` option causes the function to return either `:ok`, an `{:ok, receipt_id}` tuple, or a `{:ok, %DeliveryReport{}}` struct depending on how much acknowledgment is requested. Callers cannot pattern-match uniformly on the result.
- The `:as` option makes the function return a `%SalesReport{}` struct, a CSV binary string, or a list of plain maps depending on the caller's choice. These are structurally incompatible and cannot be handled uniformly downstream.
- The `:aggregate` option changes the return from a list of `%StockEntry{}` structs, to a single integer count, to a keyword list of totals per warehouse. Each return type is used differently, and callers must track the option to handle results safely.

Exemplo:
```elixir
# VALIDATION: SMELL START - Alternative Return Types
  # VALIDATION: This is a smell because depending on the :as option the caller
  # VALIDATION: receives a %User{} struct, a plain Map, or a Keyword list.
  # VALIDATION: All three are structurally different; downstream code that
  # VALIDATION: calls `get/2` must always carry knowledge of which :as value
  # VALIDATION: was used in order to safely access the returned data.
  def get(user_id, opts \\ []) when is_list(opts) do
    user = Repo.get!(User, user_id)
    fields = Keyword.get(opts, :fields, @public_fields)

    case Keyword.get(opts, :as, :struct) do
      :map ->
        Map.take(user, fields)

      :keyword ->
        user
        |> Map.take(fields)
        |> Enum.into([])

      _ ->
        user
    end
  end
  # VALIDATION: SMELL END
```

## 19. Accessing non-existent Map/Struct fields  (`accessing_non_existent_map_struct_fields`)

- The function reads three fields from the `claims` map using dynamic bracket access. If any of these keys are absent from the decoded JWT payload (e.g., a non-standard token omits `:role`), `nil` is returned silently. The guards and comparisons that follow then operate on `nil`, making a malformed token appear valid or producing incorrect authorization decisions without any error being raised.
- The function reads three operational parameters from a plain map using dynamic bracket access. If any key is missing (e.g., a caller builds the map without `:toll_total`), the expression silently evaluates with `nil`, causing arithmetic operations to return `nil` and propagate an invisible error through the cost model. A missing key is indistinguishable from one explicitly bound to `nil`.
- The function reads user notification preferences from a plain map using dynamic bracket access. If a user record has no `:quiet_hours` key (e.g., the field was never filled in the database), `nil` is returned without any indication that the key is missing. Downstream code that tries to pattern-match or compare the quiet-hours range with the current time then operates on `nil`, silently skipping the quiet-hours guard and sending notifications at prohibited times.
- The function uses dynamic bracket access to read three filtering options from a plain map. If the caller omits `:include_refunds` (which is a boolean flag), `nil` is returned instead of a clear error or a documented default. The `if` branch that follows treats `nil` as falsy, which happens to behave correctly by accident—making the bug invisible until a developer explicitly sets the key to `false` expecting that to match the `nil` behaviour and is surprised by an equivalent result from a different code path.
- The function uses dynamic access (`discount[:type]`, `discount[:value]`, `discount[:cap]`) on a plain map whose keys may or may not be present. When `:cap` is absent the expression silently returns `nil`, which is then fed into a numeric comparison, producing a misleading result instead of a clear error.

Exemplo:
```elixir
# VALIDATION: SMELL START - Accessing non-existent Map/Struct fields
    # VALIDATION: This is a smell because `promotion[:requires_coupon_code]` uses
    # dynamic bracket access on a plain map. If the key is absent, `nil` is returned,
    # which is treated as falsy in the `if` guard. As a result, promotions that should
    # require a coupon code silently skip the check entirely when the field is missing
    # from the map — a promotion intended to be coupon-gated becomes freely applicable.
    requires_coupon_code = promotion[:requires_coupon_code]
    # VALIDATION: SMELL END
```

## 20. Speculative Assumptions  (`speculative_assumptions`)

- `parse_amount/1` silently falls back to `0.0` when the string cannot be parsed as a valid monetary amount. Instead of crashing on unexpected input, it returns a plausible-looking but incorrect value, hiding errors from the caller and upstream billing logic.
- `resolve_carrier/1` returns a default carrier atom (`:unknown`) when the shipment data does not match any known carrier pattern. This speculative fallback allows processing to continue silently with an invalid carrier, rather than crashing and letting the supervisor handle the unexpected state.
- `parse_event_type/1` returns a generic `:unknown_event` atom instead of crashing when it receives an unrecognised event string. This speculative fallback silently lets unexpected event types flow through the notification pipeline, causing them to be silently dropped or mishandled rather than surfacing the problem to the supervisor.
- `extract_country_code/1` uses a series of heuristics to guess a country code from a phone number string. When all heuristics fail it returns `"XX"` — a fake placeholder that looks like a valid ISO code — instead of crashing. Downstream tax and compliance logic that depends on the country code will then silently operate on incorrect data.
- `parse/1` splits the raw line item string on `"|"` and extracts each field

Exemplo:
```elixir
# VALIDATION: SMELL START - Speculative Assumptions
  # VALIDATION: This is a smell because `parse_event_type/1` speculatively
  # returns `:unknown_event` for any string that is not in the list of
  # supported events, rather than crashing. The downstream `dispatch/3`
  # function has no planned clause for `:unknown_event`, so the event will
  # either be silently dropped or trigger an unrelated error much later in
  # the pipeline. The function gives a false impression of graceful handling
  # while actually swallowing unexpected input that should have caused a
  # controlled crash at the supervisor boundary.
  def parse_event_type(type) when is_binary(type) do
    if type in @supported_events do
      String.to_atom(type)
    else
      :unknown_event
    end
  end

  def parse_event_type(_), do: :unknown_event
  # VALIDATION: SMELL END
```

## 21. Modules with Identical Names  (`modules_with_identical_names`)

- Two separate modules are both named `Billing.Invoice`. When BEAM loads both (e.g., from two different source files in the same project), only the second definition survives in memory. Any function defined exclusively in the first module becomes permanently inaccessible, leading to silent runtime failures that are hard to trace.
- Two separate source files both declare `defmodule Auth.Token`. Because BEAM loads only one module definition per name, the second file's definition silently replaces the first, making any function exclusive to the first module impossible to call — a critical failure in an authentication context.
- Two different source files both define `Notifications.Dispatcher`. The BEAM VM can only keep one module instance loaded; the second file's definition overwrites the first, silently removing any functions unique to the first definition and causing runtime errors for any caller that depends on them.
- Two separate source files both declare `defmodule Reports.Generator`. The BEAM can load only one version of a module at a time; the file compiled last wins, silently dropping all functions defined only in the other file and breaking any report pipeline that calls them.
- Two different source files both declare `defmodule Inventory.StockManager`. The BEAM can hold only one definition per module name at a time. When both files are compiled, the second definition silently replaces the first, causing `UndefinedFunctionError` for any caller that relies on functions from the overwritten definition.

Exemplo:
```elixir
# VALIDATION: SMELL START - Modules with Identical Names
  # VALIDATION: This is a smell because `Notifications.Dispatcher` is declared
  # again in `lib/notifications/batch_dispatcher.ex`. Only one version survives
  # in BEAM. `dispatch/2` may become permanently unreachable at runtime.

  @spec dispatch(payload(), keyword()) :: :ok | {:error, term()}
  def dispatch(%{channel: channel} = payload, opts \\ []) do
    with :ok <- validate_channel(channel),
         :ok <- check_preferences(payload.recipient_id, channel),
         :ok <- RateLimit.check(payload.recipient_id, channel) do
      deliver(channel, payload, opts)
    end
  end

  # VALIDATION: SMELL END
```

## 22. Unnecessary macros  (`unnecessary_macros`)

- The macro only performs arithmetic and string formatting, operations that require no compile-time transformation. A regular function would be simpler, more readable, and equally correct.
- The macro runs a plain regex match at runtime — there is no AST manipulation or compile-time benefit. A regular function achieves exactly the same result with less complexity.
- The macro computes a runtime discount by multiplying two values — a pure arithmetic operation with no compile-time meaning. This is straightforward function territory and the macro adds unnecessary complexity.
- The macro computes the difference in days between two `Date` structs using only standard runtime operations. There is nothing to evaluate at compile time; a plain function is the correct abstraction here.
- The macro masks a PAN string using only string slicing and concatenation — all runtime operations. No compile-time transformation is involved, making the macro an overcomplicated replacement for a regular function.

Exemplo:
```elixir
# VALIDATION: SMELL START - Unnecessary macros
  # VALIDATION: This is a smell because days_between/2 only calls Date.diff/2,
  # a standard runtime library function. There is no AST transformation or
  # compile-time benefit; a def function is the appropriate tool here.
  defmacro days_between(date_a, date_b) do
    quote do
      Date.diff(unquote(date_b), unquote(date_a))
    end
  end
  # VALIDATION: SMELL END
```

## 23. Dynamic atom creation  (`dynamic_atom_creation`)

- The function receives an event type string from an external webhook payload and converts it directly to an atom using `String.to_atom/1`. Because webhook event types originate from a third-party system, this is an unbounded, user-controlled input that can generate an arbitrary number of atoms, exhausting the BEAM atom table over time.
- The function converts a role string received from a JWT token payload into an atom using `String.to_atom/1`. JWT payloads originate from external clients and can carry arbitrary string values, making this an uncontrolled source of atom creation that can exhaust the BEAM atom table.
- The function converts shipment status strings returned by a third-party logistics API into atoms with `String.to_atom/1`. Since the carrier API may introduce new or misspelled status codes at any time, this creates unpredictable atom growth that the developer cannot control.
- The function converts column header strings from a user-uploaded CSV report file into atoms using `String.to_atom/1`. Since column names come from user-supplied files, they are unbounded and uncontrolled, risking atom table exhaustion under high upload volumes.
- The function converts a notification channel string from user preferences into an atom using `String.to_atom/1`. User preferences are submitted through an API and can contain arbitrary strings, leading to uncontrolled atom creation with every unique channel name a user submits.

Exemplo:
```elixir
# VALIDATION: SMELL START - Dynamic atom creation
          # VALIDATION: This is a smell because `cast_field_name/1` calls
          # `String.to_atom/1` on column headers extracted from a user-supplied
          # CSV file. Each unique column name in any uploaded file creates a
          # new atom permanently. Malicious or varied files can exhaust the atom
          # table without any upper bound.
          {cast_field_name(header), value}
          # VALIDATION: SMELL END
```

## 24. Comments  (`comments`)

- The function is documented entirely through inline explanatory comments instead of using Elixir's `@doc` attribute. This bypasses the standard documentation system, making the documentation invisible to tools like `mix docs`, `IEx.h/1`, and ExDoc.
- The function relies entirely on block comments above its definition to explain its behavior, parameters, and return values, rather than using `@doc`. This prevents the documentation from being accessible via `IEx.h/1` or generated by ExDoc.
- Explanatory comments are used in place of `@doc` to describe the function's purpose, side effects, and return values. This makes the documentation inaccessible through standard Elixir tooling.
- The function's concurrency contract, side effects, and error cases are documented through inline explanatory comments rather than `@doc`, bypassing Elixir's standard documentation mechanism.
- The function uses explanatory inline comments to describe its parameters, retry logic, and return values instead of an `@doc` attribute, making the documentation hidden from Elixir tooling.

Exemplo:
```elixir
# VALIDATION: SMELL START - Comments
  # VALIDATION: This is a smell because `update_status/3` is documented
  # VALIDATION: entirely through `#` comments. `@doc` should be used so the
  # VALIDATION: documentation is available via `h/1` in IEx and ExDoc output.
  def update_status(tracking_number, new_status, metadata \\ %{})
      when is_binary(tracking_number) do
    # VALIDATION: SMELL END
```

## 25. Long Parameter List  (`long_parameter_list`)

- The function accepts 12 individual parameters instead of grouping related data (customer info, billing address, line-item details) into structs or maps, making the interface confusing and error-prone.
- The function lists 13 individual parameters covering sender, receiver, package dimensions, and options. These naturally belong to grouped structs, and the flat list makes the call site fragile and hard to read.
- Ten positional parameters mix recipient identity, channel configuration, content, and delivery options in a single function signature. These concerns belong in typed structs or a single options map.
- The function requires 11 separate positional parameters covering date ranges, filters, grouping, formatting, and export options. These naturally belong in a report-options struct or keyword list.
- The function accepts 12 flat parameters covering product identity, pricing, stock, dimensions, and metadata. These naturally belong in a `Product` struct or an attrs map, not a flat positional list.

Exemplo:
```elixir
# VALIDATION: SMELL START - Long Parameter List
  # VALIDATION: This is a smell because 13 positional parameters are used instead of
  # VALIDATION: grouping sender info, receiver info, and package specs into dedicated structs.
  def register_shipment(
        sender_name,
        sender_address,
        sender_city,
        sender_country,
        receiver_name,
        receiver_address,
        receiver_city,
        receiver_country,
        weight_kg,
        width_cm,
        height_cm,
        depth_cm,
        carrier
      ) do
    # VALIDATION: SMELL END
```

## 26. Long Function  (`long_function`)

- The `login/2` function is overloaded with credential validation, brute-force lockout checking, password hashing comparison, session token generation, last-login timestamp updating, and audit logging — all serialized in one long body. Each step represents a separate concern that deserves its own focused function.
- The `process/1` function conflates shipment validation, carrier selection logic, label generation, warehouse task creation, status transitions, and event broadcasting into one unbroken body. Each of those phases is a distinct responsibility and should live in its own helper function.
- The `dispatch/2` function handles preference lookup, channel routing, message template rendering, rate-limit checking, delivery via multiple channels, and result logging all inline. Each channel's delivery logic is embedded in the same body, making the function exceed any reasonable size and responsibility boundary.
- The `receive_purchase_order/2` function handles PO validation, line-item reconciliation, stock level updates, low-stock threshold checks, supplier acknowledgement recording, and event emission in one monolithic block. Each phase is a cohesive sub-task that warrants its own helper function.
- The `charge/3` function performs idempotency key checking, payment method validation, fraud scoring, gateway charge dispatch, transaction recording, order status update, and receipt email triggering all in a single body. Each of these is a discrete step with its own logic, and the function grows well beyond what any single function should do.

Exemplo:
```elixir
# VALIDATION: SMELL START - Long Function
  # VALIDATION: This is a smell because `login/2` performs credential lookup,
  # VALIDATION: lockout enforcement, password verification, session creation,
  # VALIDATION: last-login update, and audit logging all in a single function body,
  # VALIDATION: making it difficult to test or reason about each step independently.
  def login(email, password) when is_binary(email) and is_binary(password) do
    email_normalized = String.downcase(String.trim(email))

    case Repo.get_by(User, email: email_normalized) do
      nil ->
        Logger.warning("Login attempt for unknown email: #{email_normalized}")
        {:error, :invalid_credentials}

      %User{} = user ->
        # --- Check lockout ---
        lockout_threshold = DateTime.add(DateTime.utc_now(), -@lockout_minutes * 60, :second)

        if user.failed_attempts >= @max_failed_attempts and
             DateTime.compare(user.last_failed_at, lockout_threshold) == :gt do
          remaining =
            DateTime.diff(
              DateTime.add(user.last_failed_at, @lockout_minutes * 60, :second),
              DateTime.utc_now(),
              :second
            )

          Logger.warning("Account locked for #{email_normalized}, #{remaining}s remaining")
          {:error, {:account_locked, remaining}}
        else
          # --- Verify password ---
          if Crypto.secure_compare(
               Bcrypt.hash_pwd_salt(password),
               user.password_hash
             ) do
            # --- Reset failed attempts and update last_login ---
            user
            |> User.changeset(%{
              failed_attempts: 0,
              last_failed_at: nil,
              last_login_at: DateTime.utc_now()
            })
            |> Repo.update()

            # --- Generate session token ---
            token = :crypto.strong_rand_bytes(32) |> Base.url_encode64(padding: false)

            expires_at =
              DateTime.utc_now()
              |> DateTime.add(@session_ttl_hours * 3600, :second)
              |> DateTime.truncate(:second)

            session_attrs = %{
              user_id: user.id,
              token: token,
              expires_at: expires_at,
              ip_address: nil,
              user_agent: nil
            }

            case Repo.insert(Session.changeset(%Session{}, session_attrs)) do
              {:ok, session} ->
                AuditLog.record(%{
                  user_id: user.id,
                  action: "login_success",
                  metadata: %{session_id: session.id}
                })

                Logger.info("User #{user.id} logged in successfully")
                {:ok, %{token: token, expires_at: expires_at, user: user}}

              {:error, changeset} ->
                Logger.error("Session insert failed for user #{user.id}: #{inspect(changeset.errors)}")
                {:error, :session_creation_failed}
            end
          else
            # --- Record failed attempt ---
            user
            |> User.changeset(%{
              failed_attempts: (user.failed_attempts || 0) + 1,
              last_failed_at: DateTime.utc_now()
            })
            |> Repo.update()

            AuditLog.record(%{
              user_id: user.id,
              action: "login_failure",
              metadata: %{reason: "bad_password"}
            })

            Logger.warning("Failed login for user #{user.id}")
            {:error, :invalid_credentials}
          end
        end
    end
  end
  # VALIDATION: SMELL END
```

## 27. Primitive Obsession  (`primitive_obsession`)

- The domain concepts of `Money` (amount + currency), `TaxRate` (percentage + jurisdiction), and `Discount` (type + value) are all represented as raw `float` and `string` primitives instead of dedicated structs or types. This makes the code fragile, ambiguous, and hard to validate — e.g., there is no guarantee that `amount` is non-negative, that `currency` is a valid ISO code, or that `discount_type` is one of a known set of values.
- A user's postal address is decomposed into five separate raw `string` parameters (`street`, `city`, `state`, `zip_code`, `country`) and a phone number is a raw `string`, instead of being wrapped in dedicated `Address` and `PhoneNumber` structs. This spreads address-handling logic all over the module, makes function signatures unwieldy, and removes any structural validation at the type boundary.
- Geographic coordinates (latitude + longitude) are a cohesive domain concept, yet they are consistently passed as two separate `float` primitives throughout the module. A `Coordinate` struct would allow validation of valid ranges (lat −90..90, lon −180..180), enable cleaner function signatures, and make the intent of each argument self-documenting.
- An authentication token is a rich domain concept encompassing a raw string value, a scope, an expiry timestamp, and an issuer — yet every function represents it as a loose collection of primitives (`string`, `integer`, `string`). A `Token` struct would group these cohesively, enable pattern matching, and prevent callers from accidentally passing, for example, an expiry integer where a scope string is expected.
- A reporting period is a cohesive domain concept (a start date and an end date that must be ordered, non-future, and within a maximum span), yet it is split into two separate `Date` primitives everywhere. A `DateRange` struct would co-locate the validation rules, guarantee internal consistency (start ≤ end), and reduce the function arity across the module.

Exemplo:
```elixir
# VALIDATION: SMELL START - Primitive Obsession
  # VALIDATION: This is a smell because a geographic coordinate is a cohesive
  # domain value (latitude + longitude together define one point), yet it is
  # broken into two separate `float` parameters (`lat1`, `lon1`, `lat2`, `lon2`)
  # in every function. A dedicated `Coordinate` struct (or `%{lat: float, lon: float}`)
  # would make the intent clear, prevent argument-order mistakes, and centralise
  # range validation. Callers can easily swap lat/lon and the compiler gives no warning.
  @spec calculate_distance(float(), float(), float(), float()) :: float()
  def calculate_distance(lat1, lon1, lat2, lon2)
      when is_float(lat1) and is_float(lon1) and is_float(lat2) and is_float(lon2) do
    dlat = deg_to_rad(lat2 - lat1)
    dlon = deg_to_rad(lon2 - lon1)

    a =
      :math.sin(dlat / 2) * :math.sin(dlat / 2) +
        :math.cos(deg_to_rad(lat1)) * :math.cos(deg_to_rad(lat2)) *
          :math.sin(dlon / 2) * :math.sin(dlon / 2)

    c = 2 * :math.atan2(:math.sqrt(a), :math.sqrt(1 - a))
    @earth_radius_km * c
  end

  @spec find_nearest_depot(float(), float(), String.t()) ::
          {:ok, map()} | {:error, String.t()}
  def find_nearest_depot(lat, lon, vehicle_type)
      when is_float(lat) and is_float(lon) and is_binary(vehicle_type) do
    with :ok <- validate_coordinates(lat, lon) do
      nearest =
        @depots
        |> Enum.map(fn depot ->
          dist = calculate_distance(lat, lon, depot.lat, depot.lon)
          Map.put(depot, :distance_km, dist)
        end)
        |> Enum.filter(fn d -> d.distance_km <= @max_route_distance_km end)
        |> Enum.min_by(& &1.distance_km, fn -> nil end)

      case nearest do
        nil -> {:error, "no_depot_within_range"}
        depot -> {:ok, depot}
      end
    end
  end

  @spec build_waypoint(float(), float(), map()) :: map()
  def build_waypoint(lat, lon, metadata)
      when is_float(lat) and is_float(lon) and is_map(metadata) do
    %{
      lat: lat,
      lon: lon,
      label: Map.get(metadata, :label, "unknown"),
      arrival_window: Map.get(metadata, :arrival_window),
      priority: Map.get(metadata, :priority, :normal),
      inserted_at: DateTime.utc_now()
    }
  end
  # VALIDATION: SMELL END
```

## 28. Shotgun Surgery  (`shotgun_surgery`)
- A single conceptual change (e.g., adding a new variant/plan/type) forces small edits scattered across many functions or modules.


Exemplo:
```elixir
# VALIDATION: SMELL START - Shotgun Surgery [location 1 of 4]
  # VALIDATION: This is a smell because adding a new plan (e.g., :business)
  # requires a new clause here AND in get_api_quota_per_month/1, get_data_retention_days/1,
  # and calculate_monthly_rate/2 — four scattered changes for one new plan.
  def get_seat_limit(:starter),    do: 5
  def get_seat_limit(:growth),     do: 25
  def get_seat_limit(:enterprise), do: 500
  def get_seat_limit(_),           do: 1
  # VALIDATION: SMELL END
```

## 29. Duplicated Code  (`duplicated_code`)

- The logic for validating an address struct (checking required fields, normalising the postal code, verifying the country code is supported) is written out in full in both functions instead of being extracted into a shared helper.
- The password-strength checks (minimum length, uppercase, digit, special character) are duplicated across both functions rather than being extracted into a shared private validator.
- Both delivery functions independently build the recipient's full name, resolve the locale, and interpolate the message template. This formatting pipeline is duplicated instead of being shared.
- Both functions independently iterate over requested items, look up current stock, account for already-reserved quantities, and compare against the requested amount. This availability-check logic is duplicated instead of being shared.
- Both functions independently fetch the exchange rate, apply it to convert the amount to the gateway's base currency (USD), and round the result. This currency-conversion block is duplicated instead of extracted into a shared helper.

Exemplo:
```elixir
# VALIDATION: SMELL START - Duplicated Code
         # VALIDATION: This is a smell because the business-hours check, blocked-date
         # check, and conflict check below are reproduced verbatim in `reschedule/3`.
         :ok <- check_business_hours(start_dt),
         :ok <- check_not_blocked(provider_id, DateTime.to_date(start_dt)),
         :ok <- check_no_conflict(provider_id, start_dt) do
          # VALIDATION: SMELL END
```

## 30. Feature Envy  (`feature_envy`)
- A function uses data and functions of another module more than those of its own module, so it would belong in that other module.


Exemplo:
```elixir
# VALIDATION: SMELL START - Feature Envy
  # VALIDATION: This is a smell because `calculate_line_total/1` retrieves the struct
  # VALIDATION: via `InvoiceItem.get!/1` and then calls three more functions on that
  # VALIDATION: module: `get_tax_rate/1`, `get_discount/1`, and `get_surcharge/1`.
  # VALIDATION: All data and behaviour originate in `InvoiceItem`; this function
  # VALIDATION: contributes nothing from `InvoiceSummary` itself and should live in
  # VALIDATION: `InvoiceItem`.
  defp calculate_line_total(line_id) do
    line     = InvoiceItem.get!(line_id)
    base     = Decimal.mult(line.unit_price, Decimal.new(line.quantity))
    tax_rate = InvoiceItem.get_tax_rate(line)
    discount = InvoiceItem.get_discount(line)
    surcharge = InvoiceItem.get_surcharge(line)

    taxed      = Decimal.mult(base, Decimal.add(Decimal.new("1"), tax_rate))
    discounted = Decimal.sub(taxed, Decimal.mult(taxed, discount))
    Decimal.add(discounted, surcharge)
  end
  # VALIDATION: SMELL END
```

## 31. Divergent Change  (`divergent_change`)

- The `OrderManager` module bundles three completely unrelated responsibilities — order lifecycle management, shipping logistics, and invoice generation — meaning it must change for entirely different business reasons (e.g., a new carrier integration, a new tax rule, or an invoicing format change).
- `EmployeeService` conflates three unrelated concerns — HR (hiring/personal info), payroll (salary computation/payslips), and scheduling (shift planning) — so it must change whenever HR policies, payroll rules, or scheduling logic evolve, for completely independent reasons.
- `ProductCatalog` combines three distinct responsibilities — product catalog/metadata management, pricing and discount logic, and inventory/stock control — forcing changes for completely unrelated reasons (a new pricing strategy, a warehouse policy change, or an SEO metadata update).
- `ReportEngine` conflates three independent concerns — data fetching, output formatting, and delivery — into one module. Changes to a new data source, a new export format, or a new delivery channel each force modifications to this single module for entirely unrelated reasons.
- `CustomerService` merges CRM/profile management, support-ticket workflows, and billing operations into one module. Each concern has its own evolution lifecycle (GDPR changes affect profiles, SLA policies affect tickets, payment-provider changes affect billing), forcing unrelated edits to a single module.

Exemplo:
```elixir
# VALIDATION: SMELL START - Divergent Change
  # VALIDATION: This is a smell because this module has at least three unrelated
  # reasons to change: (1) authentication policies, (2) profile management rules,
  # and (3) email/notification delivery. Any of these concerns may evolve
  # independently, causing unrelated parts of this module to be modified.

  ## ── Authentication ──────────────────────────────────────────────────────────

  @doc """
  Authenticates a user by email and password.
  Returns `{:ok, user}` on success or `{:error, reason}` on failure.
  """
  def authenticate(email, password) when is_binary(email) and is_binary(password) do
    case Repo.get_by(User, email: String.downcase(email)) do
      nil ->
        Argon2.no_user_verify()
        {:error, :invalid_credentials}

      %User{locked_at: locked_at} = user when not is_nil(locked_at) ->
        {:error, :account_locked}

      user ->
        if Argon2.verify_pass(password, user.password_hash) do
          Repo.update!(User.changeset(user, %{last_signed_in_at: DateTime.utc_now(), failed_attempts: 0}))
          {:ok, user}
        else
          attempts = (user.failed_attempts || 0) + 1
          updates = if attempts >= 5,
            do: %{failed_attempts: attempts, locked_at: DateTime.utc_now()},
            else: %{failed_attempts: attempts}
          Repo.update!(User.changeset(user, updates))
          {:error, :invalid_credentials}
        end
    end
  end

  @doc """
  Rotates the API key for the given user, invalidating the previous one.
  """
  def rotate_api_key(%User{} = user) do
    new_key = :crypto.strong_rand_bytes(32) |> Base.url_encode64(padding: false)
    user
    |> User.changeset(%{api_key: new_key, api_key_issued_at: DateTime.utc_now()})
    |> Repo.update()
  end

  ## ── Profile Management ──────────────────────────────────────────────────────

  @doc """
  Updates mutable profile fields for the given user.
  """
  def update_profile(%User{} = user, attrs) do
    allowed = [:display_name, :bio, :locale, :timezone, :phone_number]
    filtered = Map.take(attrs, allowed)

    user
    |> User.changeset(filtered)
    |> Repo.update()
  end

  @doc """
  Replaces the user's avatar with a new upload.
  Deletes the old avatar from object storage if one exists.
  """
  def change_avatar(%User{} = user, %Plug.Upload{} = upload) do
    with {:ok, key} <- MyApp.Storage.upload(upload, prefix: "avatars/#{user.id}"),
         {:ok, url} <- MyApp.Storage.public_url(key) do
      if user.avatar_key, do: MyApp.Storage.delete(user.avatar_key)

      user
      |> User.changeset(%{avatar_url: url, avatar_key: key})
      |> Repo.update()
    end
  end

  ## ── Email Notifications ─────────────────────────────────────────────────────

  @doc """
  Sends a welcome email to a newly registered user.
  """
  def send_welcome_email(%User{} = user) do
    user
    |> MyApp.Emails.welcome(user.email, user.display_name)
    |> Mailer.deliver_later()
  end

  @doc """
  Generates a password-reset token and emails it to the user.
  Token expires after 2 hours.
  """
  def send_password_reset(%User{} = user) do
    token = Phoenix.Token.sign(MyAppWeb.Endpoint, "password_reset", user.id)
    expires_at = DateTime.add(DateTime.utc_now(), 7_200, :second)

    Repo.update!(User.changeset(user, %{
      reset_token: token,
      reset_token_expires_at: expires_at
    }))

    user
    |> MyApp.Emails.password_reset(user.email, token)
    |> Mailer.deliver_later()
  end

  # VALIDATION: SMELL END
```

## 32. Inappropriate Intimacy  (`inappropriate_intimacy`)

- `Invoice.generate/2` calls `Account.find/1` and `TaxProfile.for_account/1` and then directly accesses internal fields of the returned structs (`.billing_address`, `.vat_number`, `.rate`, `.country_code`), creating tight coupling with the internals of `Account` and `TaxProfile`. These details should be encapsulated behind dedicated functions in those modules.
- `SessionManager.create/2` calls `User.fetch_by_email/1` and `Role.primary_for/1` and then directly reads internal fields of those returned structs (`.hashed_password`, `.locked_at`, `.permissions`, `.scope`). This creates excessive coupling: the session creation logic knows internal details that should be encapsulated in the `User` and `Role` modules themselves.
- `NotificationDispatcher.dispatch/2` calls `Subscription.for_user/1` and `Plan.find/1` and then directly accesses internal fields of those structs (`.notify_via`, `.digest_hour`, `.tier`, `.feature_flags`) to decide routing logic. This couples the dispatcher tightly to internal details of `Subscription` and `Plan` that should be hidden behind well-defined interfaces.
- `StockAllocator.reserve/3` fetches a `Warehouse` struct and a `Product` struct and directly reads their internal fields (`.storage_zones`, `.hazmat_licensed`, `.requires_cold_chain`, `.storage_class`) to make allocation decisions. This logic couples the allocator tightly to the internal data model of both `Warehouse` and `Product`, which should instead expose this information through dedicated query functions.
- `RefundProcessor.process/2` calls `Transaction.fetch/1` and `Merchant.find/1` and then directly reads internal struct fields (`.gateway_reference`, `.captured_at`, `.settled`, `.refund_policy`, `.gateway_id`) to apply business rules. The refund policy and settlement status are internal concerns of `Transaction` and `Merchant` that should be exposed via their own functions.

Exemplo:
```elixir
# VALIDATION: SMELL START - Inappropriate Intimacy
    # VALIDATION: This is a smell because generate/2 fetches internal structs from Account
    # and TaxProfile and then directly reads their internal fields (.billing_address,
    # .vat_number, .rate, .country_code) instead of delegating those queries
    # to the owning modules via dedicated functions.
    account     = Account.find(account_id)
    tax_profile = TaxProfile.for_account(account)

    billing_address = account.billing_address
    vat_number      = account.vat_number

    tax_rate    = tax_profile.rate
    tax_country = tax_profile.country_code
    # VALIDATION: SMELL END
```

## 33. Large Class (Large Module)  (`large_class`)

- `BillingManager` handles at least four unrelated business concerns — invoice lifecycle, tax calculation, payment processing, and subscription management — inside a single module. Each concern has its own distinct set of functions that could be cohesively grouped in separate modules (e.g., `Invoice`, `TaxCalculator`, `PaymentProcessor`, `SubscriptionManager`), reducing the size and increasing cohesion.
- `AuthManager` mixes registration, credential verification, JWT token lifecycle, password reset flows, account locking, and audit logging. These are separate concerns that each warrant their own module (e.g., `Registration`, `TokenStore`, `PasswordReset`, `AccountLock`, `AuthAudit`), making the current single module poorly cohesive and hard to maintain.
- `NotificationService` handles e-mail dispatch, SMS, push notifications, in-app broadcasts, user preference management, scheduled delivery, and delivery analytics — all unrelated delivery channels and concerns forced into one module. Each channel and each cross-cutting concern (preferences, scheduling, analytics) deserves its own cohesive module.
- `InventoryManager` combines stock receiving, reservation lifecycle, automated reordering, dynamic pricing, movement audit logging, and reporting into one module. These represent distinct inventory sub-domains — each of which should be its own module (e.g., `StockReceiver`, `ReservationStore`, `ReorderPolicy`, `PricingEngine`, `MovementLog`, `InventoryReports`).
- `OrderManager` merges order creation, lifecycle transitions (confirm → fulfill → ship → cancel), return handling, total calculation, coupon application, and customer status notifications into one module. These are at least five distinct business concerns that should each live in a dedicated module (e.g., `OrderPlacement`, `Fulfillment`, `ReturnPolicy`, `Pricing`, `OrderNotifier`).

Exemplo:
```elixir
# VALIDATION: SMELL START - Large Module (Large Class)
# VALIDATION: This is a smell because BillingManager handles invoice generation,
# tax computation, email notifications, payment retrying, PDF generation,
# and account lifecycle management — all unrelated business rules crammed
# into one module, making it large and non-cohesive.
defmodule BillingManager do
  @moduledoc """
  Manages billing operations for customer accounts.
  """

  require Logger

  alias MyApp.Repo
  alias MyApp.Accounts.Account
  alias MyApp.Billing.{Invoice, LineItem, PaymentAttempt}
  alias MyApp.Mailer

  @tax_rate_default 0.18
  @overdue_threshold_days 30
  @max_retry_attempts 3

  # --- Invoice Generation ---

  def generate_invoice(account_id, period) do
    account = Repo.get!(Account, account_id)

    line_items = calculate_line_items(account.subscriptions)
    subtotal = Enum.reduce(line_items, Decimal.new(0), &Decimal.add(&2, &1.amount))
    tax = apply_tax(subtotal, account.country_code)
    total = Decimal.add(subtotal, tax)

    invoice = %Invoice{
      account_id: account_id,
      period_start: period.start,
      period_end: period.end,
      subtotal: subtotal,
      tax: tax,
      total: total,
      status: :pending,
      issued_at: DateTime.utc_now(),
      due_at: DateTime.add(DateTime.utc_now(), 30 * 24 * 3600, :second)
    }

    case Repo.insert(invoice) do
      {:ok, saved} ->
        Logger.info("Invoice #{saved.id} generated for account #{account_id}")
        {:ok, saved}

      {:error, changeset} ->
        Logger.error("Failed to generate invoice: #{inspect(changeset.errors)}")
        {:error, changeset}
    end
  end

  def calculate_line_items(subscriptions) do
    Enum.map(subscriptions, fn sub ->
      %LineItem{
        description: sub.plan_name,
        quantity: sub.seat_count,
        unit_price: sub.unit_price,
        amount: Decimal.mult(sub.unit_price, sub.seat_count)
      }
    end)
  end

  # --- Tax Computation ---

  def apply_tax(subtotal, "BR"), do: Decimal.mult(subtotal, Decimal.from_float(0.20))
  def apply_tax(subtotal, "US"), do: Decimal.mult(subtotal, Decimal.from_float(0.08))
  def apply_tax(subtotal, "DE"), do: Decimal.mult(subtotal, Decimal.from_float(0.19))
  def apply_tax(subtotal, _), do: Decimal.mult(subtotal, Decimal.from_float(@tax_rate_default))

  # --- Email Delivery ---

  def send_invoice_email(%Invoice{} = invoice, %Account{} = account) do
    pdf_path = generate_pdf_path(invoice)

    email_body = """
    Dear #{account.name},

    Please find attached your invoice ##{invoice.id} for the period
    #{Date.to_string(invoice.period_start)} to #{Date.to_string(invoice.period_end)}.

    Total due: #{invoice.total} — due by #{Date.to_string(invoice.due_at)}.

    Thank you for your business.
    """

    case Mailer.send(%{
           to: account.billing_email,
           subject: "Invoice ##{invoice.id} — #{account.name}",
           body: email_body,
           attachment: pdf_path
         }) do
      :ok ->
        Logger.info("Invoice email sent to #{account.billing_email}")
        :ok

      {:error, reason} ->
        Logger.error("Failed to send invoice email: #{inspect(reason)}")
        {:error, reason}
    end
  end

  def generate_pdf_path(%Invoice{id: id}) do
    "/var/invoices/#{id}.pdf"
  end

  # --- Payment Processing ---

  def mark_as_paid(%Invoice{} = invoice, paid_at \\ DateTime.utc_now()) do
    invoice
    |> Invoice.changeset(%{status: :paid, paid_at: paid_at})
    |> Repo.update()
  end

  def record_payment_attempt(%Invoice{} = invoice, gateway, result) do
    attempt = %PaymentAttempt{
      invoice_id: invoice.id,
      gateway: gateway,
      result: result,
      attempted_at: DateTime.utc_now()
    }

    Repo.insert(attempt)
  end

  def retry_failed_payments(account_id) do
    invoices =
      Repo.all(
        from i in Invoice,
          where: i.account_id == ^account_id and i.status == :failed,
          where: i.retry_count < @max_retry_attempts
      )

    Enum.each(invoices, fn invoice ->
      case MyApp.PaymentGateway.charge(invoice) do
        {:ok, _} ->
          mark_as_paid(invoice)

        {:error, reason} ->
          Logger.warning("Retry failed for invoice #{invoice.id}: #{inspect(reason)}")

          invoice
          |> Invoice.changeset(%{retry_count: invoice.retry_count + 1})
          |> Repo.update()
      end
    end)
  end

  # --- Account Lifecycle ---

  def suspend_account(account_id) do
    account = Repo.get!(Account, account_id)

    account
    |> Account.changeset(%{status: :suspended, suspended_at: DateTime.utc_now()})
    |> Repo.update()
    |> case do
      {:ok, updated} ->
        Logger.info("Account #{account_id} suspended due to non-payment")
        {:ok, updated}

      {:error, cs} ->
        {:error, cs}
    end
  end

  def list_overdue_accounts(as_of \\ Date.utc_today()) do
    threshold = Date.add(as_of, -@overdue_threshold_days)

    Repo.all(
      from i in Invoice,
        where: i.status == :pending,
        where: i.due_at < ^threshold,
        distinct: i.account_id,
        select: i.account_id
    )
  end
end
# VALIDATION: SMELL END
```

## 34. Speculative Generality  (`speculative_generality`)

- The function destructures `invoice_type` from the invoice struct and feeds it into a `case` expression, but the `case` contains only a wildcard (`_`) clause. The intent was to support different volume-discount rates per invoice type in the future, but that flexibility was never implemented — the system always applies the same 5% rate regardless of type.
- The `Logistics.RouteOptimizer` module was created speculatively to support dynamic carrier routing based on delivery zones. In practice, `Logistics.CarrierSelector` — the only module that should consume it — never calls any of its functions. The module exists solely for anticipated future use that never materialized.
- `build_rich_payload/2` was written speculatively to support rich push notifications containing images and action buttons, but no caller in the system ever invokes it. The dispatcher always uses `build_simple_payload/1` for every outbound notification. The rich-payload function is dead code that inflates the module without providing value.
- The function destructures `payment_method` and branches on it through a `case` expression, but every branch returns the same percentage (2.9%). The branching structure was written speculatively to accommodate per-method fee schedules in the future, but no differentiation was ever implemented. The code gives the illusion of complexity while applying a uniform fee.
- The `suspend/3` function defines a `notify_user` parameter with a default of `true`. The intent was to allow callers to suppress the user-facing notification in certain scenarios (e.g., silent fraud-detection suspensions). In practice, every call site in the codebase omits the argument and relies on the default. No caller ever passes `false`, making the parameter dead speculative flexibility.

Exemplo:
```elixir
# VALIDATION: SMELL START - Speculative Generality
  # VALIDATION: This is a smell because `output_format \\ :pdf` was added
  # speculatively to support future invoice formats (e.g. :html, :csv), but
  # every call site—regenerate/1, bulk_generate/1, and external callers—always
  # omits the second argument. The parameter has never been exercised with any
  # value other than its default.
  def generate_invoice(order, output_format \\ :pdf) do
  # VALIDATION: SMELL END
```

## 35. Switch Statements  (`switch_statements`)

- The same `case payment_method` branching logic over `:credit_card`, `:debit_card`, `:bank_transfer`, and `:pix` is duplicated verbatim across three separate functions. Adding a new payment method requires modifying all three independently, which is the hallmark of the Switch Statements smell.
- The same `case role` branching over `:admin`, `:manager`, `:analyst`, and `:viewer` is reproduced in three distinct functions. When a new role is added, all three case expressions must be updated in parallel, which is the Switch Statements smell.
- The same `case channel` branching over `:email`, `:sms`, `:push`, and `:webhook` is duplicated in three different functions. Each new notification channel requires changes in all three locations, which is the Switch Statements smell.
- The same `case category` branching over `:electronics`, `:perishable`, `:apparel`, and `:pharmaceutical` is duplicated in three functions. Adding a new product category requires editing all three case blocks, which is the Switch Statements smell.
- The same `case status` branching over `:pending`, `:in_transit`, `:out_for_delivery`, `:delivered`, and `:returned` is repeated in three separate functions. Adding a new shipment status requires updating every case block, which is the Switch Statements smell.

Exemplo:
```elixir
# VALIDATION: SMELL START - Switch Statements
  # VALIDATION: This is a smell because the same case branching on `currency`
  # also appears in `symbol/1` below. Both enumerate :usd, :eur, :gbp, :jpy,
  # :brl — adding a new currency requires updating both case blocks.
  @spec minor_unit_factor(currency_code()) :: integer()
  def minor_unit_factor(currency) do
    case currency do
      :usd -> 100
      :eur -> 100
      :gbp -> 100
      :jpy -> 1
      :brl -> 100
    end
  end
  # VALIDATION: SMELL END
```
