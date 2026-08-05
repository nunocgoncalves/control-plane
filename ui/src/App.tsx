import {
  FormEvent,
  ReactNode,
  useCallback,
  useEffect,
  useMemo,
  useRef,
  useState,
} from "react";
import {
  createRevision,
  downloadArtifact,
  loadDashboard,
  loadDetail,
  Period,
  respondToBlocker,
  saveFeedback,
  subscribe,
  uploadArtifact,
} from "./api";
import { localized, stateLabel, t } from "./i18n";
import type {
  Blocker,
  DetailData,
  JSONSchema,
  Locale,
  ResultFieldPresentation,
  TimelineEvent,
  WorkArtifact,
  WorkItem,
  WorkState,
} from "./types";
import "./styles.css";

const states: WorkState[] = ["todo", "in_progress", "blocked", "done"];
const categoryKeys = [
  "incorrect_classification",
  "missing_information",
  "wrong_action",
  "poor_output",
] as const;

function startOfDay(date = new Date()): Date {
  const d = new Date(date);
  d.setHours(0, 0, 0, 0);
  return d;
}
function endOfDay(date = new Date()): Date {
  const d = startOfDay(date);
  d.setDate(d.getDate() + 1);
  return d;
}
function presetPeriod(preset: string): Period {
  const to = endOfDay();
  const from = startOfDay();
  if (preset === "week") {
    const day = (from.getDay() + 6) % 7;
    from.setDate(from.getDate() - day);
  }
  if (preset === "month") from.setDate(1);
  return { from, to };
}
function money(
  amount: string | number,
  currency = "EUR",
  locale: Locale = "en",
): string {
  return new Intl.NumberFormat(locale === "pt" ? "pt-PT" : "en-GB", {
    style: "currency",
    currency,
    maximumFractionDigits: 2,
  }).format(Number(amount));
}
function when(value: string | undefined, locale: Locale): string {
  if (!value) return "";
  return new Intl.DateTimeFormat(locale === "pt" ? "pt-PT" : "en-GB", {
    dateStyle: "medium",
    timeStyle: "short",
  }).format(new Date(value));
}
function sourceLabel(kind: string, locale: Locale): string {
  const labels: Record<string, [string, string]> = {
    outlook: ["Outlook", "Outlook"],
    graph_email: ["Outlook", "Outlook"],
    email: ["Email", "Email"],
    api: ["API", "API"],
    schedule: ["Scheduled", "Agendado"],
    scheduled: ["Scheduled", "Agendado"],
    artifact: ["Artifact", "Artefacto"],
  };
  const label = labels[kind];
  return label ? label[locale === "pt" ? 1 : 0] : t(locale).otherSource;
}
function blockerKindLabel(kind: string, locale: Locale): string {
  const labels: Record<string, [string, string]> = {
    information: ["Information required", "Informação necessária"],
    decision: ["Decision required", "Decisão necessária"],
    approval: ["Approval required", "Aprovação necessária"],
    artifact: ["Artifact required", "Artefacto necessário"],
    consequence_confirmation: [
      "Consequential action confirmation",
      "Confirmação de ação consequente",
    ],
  };
  return labels[kind]?.[locale === "pt" ? 1 : 0] || t(locale).actionRequired;
}
function formulaLabel(formula: string | undefined, locale: Locale): string {
  if (!formula || formula === "labor_time_saved")
    return locale === "pt"
      ? "Tempo de trabalho manual poupado"
      : "Manual handling time saved";
  return t(locale).customValueFormula;
}
function initials(name: string): string {
  return (
    name
      .split(/\s+/)
      .filter(Boolean)
      .slice(0, 2)
      .map((part) => part[0]?.toUpperCase())
      .join("") || "I"
  );
}
function safeObject(value: unknown): Record<string, unknown> {
  return value && typeof value === "object" && !Array.isArray(value)
    ? (value as Record<string, unknown>)
    : {};
}

function Mark() {
  return (
    <svg viewBox="0 0 24 24" aria-hidden="true">
      <path d="M4 5h5l3 4M4 12h6M4 19h5l3-4M12 9l4 3-4 3" />
      <circle cx="18" cy="12" r="2.4" />
    </svg>
  );
}
function SearchIcon() {
  return (
    <svg viewBox="0 0 24 24" aria-hidden="true">
      <circle cx="11" cy="11" r="8" />
      <path d="m21 21-4.3-4.3" />
    </svg>
  );
}
function CloseIcon() {
  return (
    <svg viewBox="0 0 24 24" aria-hidden="true">
      <path d="M18 6 6 18M6 6l12 12" />
    </svg>
  );
}

interface ConnectionProps {
  locale: Locale;
  setLocale: (locale: Locale) => void;
  onConnect: (token: string) => Promise<void>;
  loading: boolean;
  error: string;
}
function Connection({
  locale,
  setLocale,
  onConnect,
  loading,
  error,
}: ConnectionProps) {
  const c = t(locale);
  const [value, setValue] = useState("");
  return (
    <main className="connection-shell">
      <div className="language-switch" aria-label="Language">
        <button
          className={locale === "en" ? "active" : ""}
          onClick={() => setLocale("en")}
        >
          EN
        </button>
        <button
          className={locale === "pt" ? "active" : ""}
          onClick={() => setLocale("pt")}
        >
          PT
        </button>
      </div>
      <form
        className="connection-card"
        onSubmit={(event) => {
          event.preventDefault();
          void onConnect(value.trim());
        }}
      >
        <div className="brand-mark">
          <Mark />
        </div>
        <span className="eyebrow">Iterabase</span>
        <h1>{c.connectTitle}</h1>
        <p>{c.connectBody}</p>
        <label htmlFor="api-key">{c.apiKey}</label>
        <input
          id="api-key"
          type="password"
          autoComplete="off"
          value={value}
          onChange={(event) => setValue(event.target.value)}
          required
        />
        {error && (
          <div className="form-error" role="alert">
            {error}
          </div>
        )}
        <button className="button primary" disabled={loading || !value.trim()}>
          {loading ? c.connecting : c.connect}
        </button>
      </form>
    </main>
  );
}

interface HeaderProps {
  locale: Locale;
  setLocale: (locale: Locale) => void;
  search: string;
  setSearch: (value: string) => void;
  items: WorkItem[];
  disconnect: () => void;
}
function Header({
  locale,
  setLocale,
  search,
  setSearch,
  items,
  disconnect,
}: HeaderProps) {
  const c = t(locale);
  const active = items.filter((item) => item.state === "in_progress").length;
  const blocked = items.filter((item) => item.state === "blocked").length;
  return (
    <header className="island">
      <div className="brand-mark small">
        <Mark />
      </div>
      <span className="context">{c.dashboard}</span>
      <span className="divider" />
      <label className="global-search">
        <SearchIcon />
        <span className="sr-only">{c.search}</span>
        <input
          value={search}
          onChange={(event) => setSearch(event.target.value)}
          placeholder={c.search}
        />
      </label>
      <span className="divider" />
      <span className="live-count">
        <i />
        {active}
      </span>
      <span className={`blocked-count ${blocked ? "has-blocked" : ""}`}>
        {blocked}
      </span>
      <div className="language-switch compact">
        <button
          className={locale === "en" ? "active" : ""}
          onClick={() => setLocale("en")}
        >
          EN
        </button>
        <button
          className={locale === "pt" ? "active" : ""}
          onClick={() => setLocale("pt")}
        >
          PT
        </button>
      </div>
      <button
        className="icon-button sign-out"
        title={c.disconnect}
        aria-label={c.disconnect}
        onClick={disconnect}
      >
        ↪
      </button>
    </header>
  );
}

function ValueHero({
  data,
  locale,
  onExplain,
}: {
  data: ReturnType<typeof useDashboardData>["data"];
  locale: Locale;
  onExplain: () => void;
}) {
  const c = t(locale);
  if (!data) return null;
  const total = data.summary.value.totals[0];
  return (
    <section className="summary-grid">
      <article className="card hero">
        <div>
          <span className="eyebrow">{c.hero}</span>
          <button className="estimate-badge" onClick={onExplain}>
            ⓘ {c.estimated}
          </button>
        </div>
        <strong>
          {data.summary.value.configured && total
            ? money(total.amount, total.currency, locale)
            : "—"}
        </strong>
        <p>{data.summary.value.configured ? c.valueHelp : c.notConfigured}</p>
        <div className="status-counts">
          {states.map((state) => (
            <div key={state}>
              <span>
                <i className={`dot ${state}`} />
                {stateLabel(state, locale)}
              </span>
              <b>{data.summary.counts[state] || 0}</b>
            </div>
          ))}
        </div>
      </article>
      <article className="card trend-card">
        <div className="section-heading">
          <span className="eyebrow">{c.trend}</span>
        </div>
        <Trend data={data.summary.trend} locale={locale} />
      </article>
    </section>
  );
}
function Trend({
  data,
  locale,
}: {
  data: Array<{ date: string; amount: string; currency: string }>;
  locale: Locale;
}) {
  const c = t(locale);
  if (!data.length) return <div className="empty-chart">{c.noTrend}</div>;
  const values = data.map((point) => Math.abs(Number(point.amount)));
  const max = Math.max(...values, 1);
  return (
    <div className="trend" role="img" aria-label={c.trend}>
      {data.map((point, index) => (
        <div
          className="trend-point"
          key={`${point.date}-${point.currency}`}
          title={`${point.date}: ${money(point.amount, point.currency, locale)}`}
        >
          <span>{money(point.amount, point.currency, locale)}</span>
          <i
            style={{ height: `${Math.max(5, (values[index] / max) * 112)}px` }}
          />
          <small>
            {new Intl.DateTimeFormat(locale === "pt" ? "pt-PT" : "en-GB", {
              month: "short",
              day: "numeric",
            }).format(new Date(`${point.date}T12:00:00`))}
          </small>
        </div>
      ))}
    </div>
  );
}

function WorkCard({
  item,
  locale,
  open,
}: {
  item: WorkItem;
  locale: Locale;
  open: () => void;
}) {
  return (
    <button className={`work-card card edge-${item.state}`} onClick={open}>
      <div className="card-meta">
        <span>{item.presentation.workflowTitle}</span>
        <span>{sourceLabel(item.source.kind, locale)}</span>
      </div>
      <strong>{item.title}</strong>
      {item.state === "in_progress" && item.currentStep && (
        <div className="step-chip">
          <i />
          {localized(item.currentStep.label, locale)}
        </div>
      )}
      {item.state === "blocked" && item.blocker && (
        <div className="blocker-chip">
          <span>△</span>
          <div>
            <small>{blockerKindLabel(item.blocker.kind, locale)}</small>
            {localized(item.blocker.title, locale)}
          </div>
        </div>
      )}
      <div className="assignee">
        <span className="avatar">
          {initials(item.presentation.personaName)}
        </span>
        <span>{item.presentation.personaName}</span>
        <time>
          {when(item.finishedAt || item.startedAt || item.createdAt, locale)}
        </time>
      </div>
      {item.estimatedValue && (
        <div className={`item-value ${item.valueDisputed ? "disputed" : ""}`}>
          {money(item.estimatedValue, item.valueCurrency, locale)}{" "}
          <small>
            {item.valueDisputed ? t(locale).disputed : t(locale).estimated}
          </small>
        </div>
      )}
    </button>
  );
}

function Board({
  items,
  locale,
  open,
}: {
  items: WorkItem[];
  locale: Locale;
  open: (item: WorkItem) => void;
}) {
  const c = t(locale);
  const failed = items.filter((item) => item.state === "failed");
  return (
    <>
      {failed.length > 0 && (
        <section className="failed-strip" aria-labelledby="failed-title">
          <div>
            <span className="failure-icon">!</span>
            <div>
              <h2 id="failed-title">
                {c.failed} <b>{failed.length}</b>
              </h2>
              <p>{c.failedHelp}</p>
            </div>
          </div>
          <div className="failed-items">
            {failed.map((item) => (
              <button key={item.id} onClick={() => open(item)}>
                <span>{item.title}</span>
                <small>{c.open} →</small>
              </button>
            ))}
          </div>
        </section>
      )}
      <section className="board" aria-label={c.board}>
        {states.map((state) => {
          const column = items.filter((item) => item.state === state);
          return (
            <div className="column" key={state}>
              <div className="column-title">
                <i className={`dot ${state}`} />
                <h2>{stateLabel(state, locale)}</h2>
                <span>{column.length}</span>
              </div>
              <div className="column-items">
                {column.map((item) => (
                  <WorkCard
                    key={item.id}
                    item={item}
                    locale={locale}
                    open={() => open(item)}
                  />
                ))}
                {!column.length && (
                  <div className="empty-column">{c.empty}</div>
                )}
              </div>
            </div>
          );
        })}
      </section>
    </>
  );
}

function useDashboardData(
  token: string | null,
  period: Period,
  search: string,
) {
  const [data, setData] = useState<{
    summary: Awaited<ReturnType<typeof loadDashboard>>["summary"];
    items: WorkItem[];
  } | null>(null);
  const [error, setError] = useState("");
  const [loading, setLoading] = useState(false);
  const timer = useRef<number | undefined>(undefined);
  const refresh = useCallback(
    async (quiet = false) => {
      if (!token) return;
      if (!quiet) setLoading(true);
      try {
        setData(await loadDashboard(token, period, search));
        setError("");
      } catch (err) {
        setError(err instanceof Error ? err.message : "Error");
        throw err;
      } finally {
        setLoading(false);
      }
    },
    [token, period.from.getTime(), period.to.getTime(), search],
  );
  useEffect(() => {
    if (!token) {
      setData(null);
      return;
    }
    const id = window.setTimeout(() => {
      void refresh().catch(() => undefined);
    }, 180);
    return () => clearTimeout(id);
  }, [refresh, token]);
  useEffect(
    () =>
      token
        ? subscribe(token, () => {
            clearTimeout(timer.current);
            timer.current = window.setTimeout(() => {
              void refresh(true).catch(() => undefined);
            }, 200);
          })
        : undefined,
    [token, refresh],
  );
  return { data, error, loading, refresh };
}

function DetailPanel({
  token,
  item,
  locale,
  close,
  changed,
}: {
  token: string;
  item: WorkItem;
  locale: Locale;
  close: () => void;
  changed: () => void;
}) {
  const c = t(locale);
  const panelRef = useRef<HTMLElement>(null);
  const [detail, setDetail] = useState<DetailData | null>(null);
  const [error, setError] = useState("");
  const [notice, setNotice] = useState("");
  const [feedbackOpen, setFeedbackOpen] = useState(false);
  const [category, setCategory] = useState<string>(categoryKeys[0]);
  const [explanation, setExplanation] = useState("");
  const [retryOpen, setRetryOpen] = useState(false);
  const [guidance, setGuidance] = useState("");
  const [confirmed, setConfirmed] = useState<string[]>([]);
  const [busy, setBusy] = useState(false);
  const refresh = useCallback(async () => {
    try {
      setDetail(await loadDetail(token, item));
      setError("");
    } catch (err) {
      setError(err instanceof Error ? err.message : "Error");
    }
  }, [token, item.id]);
  useEffect(() => {
    void refresh();
  }, [refresh]);
  useEffect(() => {
    panelRef.current?.focus();
    const onKey = (event: KeyboardEvent) => {
      if (event.key === "Escape") close();
    };
    document.addEventListener("keydown", onKey);
    return () => document.removeEventListener("keydown", onKey);
  }, [close]);
  const outcomeNode = detail?.nodes
    .filter(
      (node) =>
        node.outcome &&
        node.resultPresentation?.outcomes.some(
          (outcome) => outcome.outcome === node.outcome,
        ),
    )
    .at(-1);
  const outcomePresentation = outcomeNode?.resultPresentation?.outcomes.find(
    (outcome) => outcome.outcome === outcomeNode.outcome,
  );
  const output = outcomeNode?.output;
  const resultArtifacts =
    detail?.artifacts.filter(
      (artifact) => artifact.role === "output" || artifact.role === "evidence",
    ) || [];
  async function saveOnly() {
    if (!detail) return;
    setBusy(true);
    try {
      await saveFeedback(token, detail.item, category, explanation);
      setNotice(c.feedbackSaved);
      setFeedbackOpen(false);
      await refresh();
      changed();
    } catch (err) {
      setError(err instanceof Error ? err.message : "Error");
    } finally {
      setBusy(false);
    }
  }
  async function startRetry() {
    if (!detail || !guidance.trim()) return;
    setBusy(true);
    try {
      let feedback = detail.feedback.find(
        (entry) =>
          entry.attemptId === detail.item.currentAttemptId &&
          !entry.revisedAttemptId,
      );
      if (!feedback)
        feedback = await saveFeedback(
          token,
          detail.item,
          category,
          explanation,
        );
      await createRevision(
        token,
        detail.item,
        feedback.id,
        guidance.trim(),
        detail.consequences,
      );
      setNotice(c.revisionStarted);
      setRetryOpen(false);
      setFeedbackOpen(false);
      await refresh();
      changed();
    } catch (err) {
      setError(err instanceof Error ? err.message : "Error");
    } finally {
      setBusy(false);
    }
  }
  return (
    <div
      className="overlay"
      onMouseDown={(event) => {
        if (event.target === event.currentTarget) close();
      }}
    >
      <aside
        ref={panelRef}
        tabIndex={-1}
        className="detail-panel"
        role="dialog"
        aria-modal="true"
        aria-labelledby="detail-title"
      >
        <header>
          <div>
            <span
              className={`status-badge ${detail?.item.state || item.state}`}
            >
              {stateLabel(detail?.item.state || item.state, locale)}
            </span>
            <span className="workflow-label">
              {item.presentation.workflowTitle}
            </span>
            <h2 id="detail-title">{item.title}</h2>
            <div className="panel-persona">
              <span className="avatar">
                {initials(item.presentation.personaName)}
              </span>
              {item.presentation.personaName}
              <span>·</span>
              {sourceLabel(item.source.kind, locale)}
            </div>
          </div>
          <button className="icon-button" onClick={close} aria-label={c.close}>
            <CloseIcon />
          </button>
        </header>
        <div className="panel-body">
          {!detail && !error && <div className="loading">{c.loading}</div>}
          {error && (
            <div className="form-error" role="alert">
              {error}
            </div>
          )}
          {notice && (
            <div className="notice" role="status">
              ✓ {notice}
            </div>
          )}
          {detail && (
            <>
              {detail.item.state === "failed" && (
                <Section title={c.failed}>
                  <PlainValue
                    value={detail.item.failureSummary}
                    empty={c.noDetails}
                    locale={locale}
                  />
                </Section>
              )}
              {detail.blocker && (
                <BlockerForm
                  token={token}
                  blocker={detail.blocker}
                  locale={locale}
                  onDone={async () => {
                    setNotice(c.responseSent);
                    await refresh();
                    changed();
                  }}
                />
              )}
              {detail.item.state === "done" && (
                <Section title={c.outcome}>
                  <h3>
                    {localized(outcomePresentation?.summary, locale) ||
                      c.completedSummary}
                  </h3>
                  <PresentedResult
                    value={output}
                    fields={outcomeNode?.resultPresentation?.fields || []}
                    empty={c.noDetails}
                    locale={locale}
                  />
                </Section>
              )}
              {resultArtifacts.length > 0 && (
                <Section title={c.result}>
                  <ArtifactList
                    token={token}
                    artifacts={resultArtifacts}
                    locale={locale}
                  />
                </Section>
              )}
              <Section title={c.source}>
                <div className="source-card">
                  <div>
                    <strong>{detail.item.source.title}</strong>
                    {detail.item.source.subtitle && (
                      <span>{detail.item.source.subtitle}</span>
                    )}
                  </div>
                  {detail.item.source.originalUrl && (
                    <a
                      href={detail.item.source.originalUrl}
                      target="_blank"
                      rel="noreferrer"
                    >
                      {c.openOriginal} ↗
                    </a>
                  )}
                </div>
              </Section>
              {!!detail.item.source.evidence?.length && (
                <Section title={c.evidence}>
                  <div className="evidence-grid">
                    {detail.item.source.evidence.map((field, index) => (
                      <div key={index}>
                        <small>{localized(field.label, locale)}</small>
                        <strong>{field.value}</strong>
                      </div>
                    ))}
                  </div>
                </Section>
              )}
              {detail.item.state === "done" &&
                (detail.item.estimatedValue ||
                  !detail.item.valueConfigured) && (
                  <Section title={c.value}>
                    {detail.item.estimatedValue ? (
                      <>
                        <div
                          className={`panel-value ${detail.item.valueDisputed ? "disputed" : ""}`}
                        >
                          <strong>
                            {money(
                              detail.item.estimatedValue,
                              detail.item.valueCurrency,
                              locale,
                            )}
                          </strong>
                          <span>
                            {detail.item.valueDisputed
                              ? c.disputed
                              : c.estimated}
                          </span>
                        </div>
                        <ValueModelExplanation
                          model={detail.item.valueModel}
                          locale={locale}
                        />
                      </>
                    ) : (
                      <div className="unconfigured-value">
                        <strong>{c.notConfigured}</strong>
                        <p>{c.unconfiguredBody}</p>
                      </div>
                    )}
                  </Section>
                )}
              <Section title={c.timeline}>
                <Timeline
                  events={detail.timeline}
                  nodes={detail.nodes}
                  locale={locale}
                />
              </Section>
              {detail.attempts.length > 1 && (
                <Section title={c.attempts}>
                  <div className="attempt-list">
                    {detail.attempts.map((attempt, index) => (
                      <div key={attempt.id}>
                        <span>{index === 0 ? c.original : c.revised}</span>
                        <strong>
                          {c.attempt} {attempt.number}
                        </strong>
                        <small>{when(attempt.createdAt, locale)}</small>
                        {attempt.actionableGuidance && (
                          <p>{attempt.actionableGuidance}</p>
                        )}
                      </div>
                    ))}
                  </div>
                </Section>
              )}
            </>
          )}
        </div>
        {detail?.item.state === "done" && (
          <footer className="feedback-footer">
            {!feedbackOpen ? (
              <button
                className="button secondary"
                onClick={() => setFeedbackOpen(true)}
              >
                ⚑ {c.feedback}
              </button>
            ) : (
              <div className="feedback-form">
                <div className="footer-title">
                  <strong>{c.whatWrong}</strong>
                  <button
                    className="icon-button"
                    aria-label={c.close}
                    onClick={() => setFeedbackOpen(false)}
                  >
                    <CloseIcon />
                  </button>
                </div>
                <div className="categories">
                  {categoryKeys.map((key) => (
                    <button
                      className={category === key ? "selected" : ""}
                      onClick={() => setCategory(key)}
                      key={key}
                    >
                      {c[key]}
                    </button>
                  ))}
                </div>
                <textarea
                  value={explanation}
                  onChange={(event) => setExplanation(event.target.value)}
                  placeholder={c.explanation}
                />
                <div className="footer-actions">
                  <button
                    className="button secondary"
                    disabled={busy}
                    onClick={() => void saveOnly()}
                  >
                    {c.saveFeedback}
                  </button>
                  <button
                    className="button primary"
                    disabled={busy}
                    onClick={() => setRetryOpen(true)}
                  >
                    {c.tryAgain}
                  </button>
                </div>
              </div>
            )}
          </footer>
        )}
        {retryOpen && detail && (
          <div className="modal-layer">
            <div className="modal" role="dialog" aria-modal="true">
              <h2>{c.tryAgain}</h2>
              <p>{c.guidance}</p>
              <textarea
                autoFocus
                value={guidance}
                onChange={(event) => setGuidance(event.target.value)}
                placeholder={c.guidanceRequired}
              />
              {detail.consequences.length > 0 && (
                <div className="consequence-box">
                  <strong>△ {c.consequences}</strong>
                  <p>{c.consequenceWarning}</p>
                  {detail.consequences.map((consequence) => (
                    <label key={consequence.invocationId}>
                      <input
                        type="checkbox"
                        checked={confirmed.includes(consequence.invocationId)}
                        onChange={(event) =>
                          setConfirmed((current) =>
                            event.target.checked
                              ? [...current, consequence.invocationId]
                              : current.filter(
                                  (id) => id !== consequence.invocationId,
                                ),
                          )
                        }
                      />
                      <span>{localized(consequence.summary, locale)}</span>
                    </label>
                  ))}
                </div>
              )}
              <div className="modal-actions">
                <button
                  className="button ghost"
                  onClick={() => setRetryOpen(false)}
                >
                  {c.cancel}
                </button>
                <button
                  className="button primary"
                  disabled={
                    busy ||
                    !guidance.trim() ||
                    confirmed.length !== detail.consequences.length
                  }
                  onClick={() => void startRetry()}
                >
                  {c.startRevision}
                </button>
              </div>
            </div>
          </div>
        )}
      </aside>
    </div>
  );
}

function Section({ title, children }: { title: string; children: ReactNode }) {
  return (
    <section className="panel-section">
      <h2>{title}</h2>
      {children}
    </section>
  );
}
function equalJSON(left: unknown, right: unknown): boolean {
  if (Object.is(left, right)) return true;
  if (Array.isArray(left) || Array.isArray(right))
    return (
      Array.isArray(left) &&
      Array.isArray(right) &&
      left.length === right.length &&
      left.every((value, index) => equalJSON(value, right[index]))
    );
  if (!left || !right || typeof left !== "object" || typeof right !== "object")
    return false;
  const leftObject = safeObject(left);
  const rightObject = safeObject(right);
  const leftKeys = Object.keys(leftObject).sort();
  const rightKeys = Object.keys(rightObject).sort();
  return (
    leftKeys.length === rightKeys.length &&
    leftKeys.every(
      (key, index) =>
        key === rightKeys[index] &&
        equalJSON(leftObject[key], rightObject[key]),
    )
  );
}
function PresentedResult({
  value,
  fields,
  empty,
  locale,
}: {
  value: unknown;
  fields: ResultFieldPresentation[];
  empty: string;
  locale: Locale;
}) {
  if (fields.length === 0)
    return <PresentedRootResult value={value} empty={empty} locale={locale} />;
  if (!value || typeof value !== "object" || Array.isArray(value))
    return <p className="muted">{empty}</p>;
  const visible = directResultFields(fields, []).some((field) =>
    Object.prototype.hasOwnProperty.call(value, field.path[0]),
  );
  if (!visible) return <p className="muted">{empty}</p>;
  return (
    <PresentedFields
      value={safeObject(value)}
      fields={fields}
      prefix={[]}
      locale={locale}
    />
  );
}
function PresentedRootResult({
  value,
  empty,
  locale,
}: {
  value: unknown;
  empty: string;
  locale: Locale;
}) {
  const values = customerValues(value);
  if (values.length === 0) return <p className="muted">{empty}</p>;
  return (
    <dl className="result-grid">
      {values.map((item, index) => (
        <div key={index}>
          <dt>
            {t(locale).resultDetail} {index + 1}
          </dt>
          <dd>{presentedScalar(item, locale)}</dd>
        </div>
      ))}
    </dl>
  );
}
function presentedScalar(value: unknown, locale: Locale): string {
  if (value === null || value === "") return "—";
  if (typeof value === "boolean") return value ? t(locale).yes : t(locale).no;
  return String(value);
}
function pathStartsWith(path: string[], prefix: string[]): boolean {
  return prefix.every((segment, index) => path[index] === segment);
}
function directResultFields(
  fields: ResultFieldPresentation[],
  prefix: string[],
): ResultFieldPresentation[] {
  return fields.filter(
    (field) =>
      field.path.length === prefix.length + 1 &&
      pathStartsWith(field.path, prefix),
  );
}
function PresentedFields({
  value,
  fields,
  prefix,
  locale,
  nested = false,
}: {
  value: Record<string, unknown>;
  fields: ResultFieldPresentation[];
  prefix: string[];
  locale: Locale;
  nested?: boolean;
}) {
  return (
    <dl className={`result-grid ${nested ? "nested" : ""}`}>
      {directResultFields(fields, prefix).flatMap((field) => {
        const key = field.path.at(-1)!;
        return Object.prototype.hasOwnProperty.call(value, key)
          ? [
              <div key={field.path.join("\u0000")}>
                <dt>{localized(field.label, locale)}</dt>
                <dd>
                  <PresentedFieldValue
                    value={value[key]}
                    field={field}
                    fields={fields}
                    locale={locale}
                  />
                </dd>
              </div>,
            ]
          : [];
      })}
    </dl>
  );
}
function PresentedFieldValue({
  value,
  field,
  fields,
  locale,
}: {
  value: unknown;
  field: ResultFieldPresentation;
  fields: ResultFieldPresentation[];
  locale: Locale;
}) {
  const option = field.options?.find((candidate) =>
    equalJSON(candidate.value, value),
  );
  if (option) return localized(option.label, locale);
  if (typeof value === "boolean") return value ? t(locale).yes : t(locale).no;
  const hasChildren = directResultFields(fields, field.path).length > 0;
  if (Array.isArray(value)) {
    if (hasChildren)
      return (
        <div className="result-records">
          {value.flatMap((entry, index) =>
            entry && typeof entry === "object" && !Array.isArray(entry)
              ? [
                  <PresentedFields
                    key={index}
                    value={safeObject(entry)}
                    fields={fields}
                    prefix={field.path}
                    locale={locale}
                    nested
                  />,
                ]
              : [],
          )}
        </div>
      );
    return (
      <ul className="result-values">
        {value.map((entry, index) => {
          const entryOption = field.options?.find((candidate) =>
            equalJSON(candidate.value, entry),
          );
          return (
            <li key={index}>
              {entryOption
                ? localized(entryOption.label, locale)
                : typeof entry === "boolean"
                  ? entry
                    ? t(locale).yes
                    : t(locale).no
                  : String(entry)}
            </li>
          );
        })}
      </ul>
    );
  }
  if (value && typeof value === "object" && hasChildren)
    return (
      <PresentedFields
        value={safeObject(value)}
        fields={fields}
        prefix={field.path}
        locale={locale}
        nested
      />
    );
  return value === null || typeof value === "object" ? "—" : String(value);
}
function customerValues(value: unknown): unknown[] {
  if (Array.isArray(value)) return value.flatMap(customerValues);
  if (value && typeof value === "object")
    return Object.entries(safeObject(value))
      .filter(([key]) => !/prompt|model|token|tool|worker|runtime/i.test(key))
      .flatMap(([, item]) => customerValues(item));
  return value === undefined ? [] : [value];
}
function PlainValue({
  value,
  empty,
  locale,
}: {
  value: unknown;
  empty: string;
  locale: Locale;
}) {
  if (value === null || typeof value !== "object")
    return value === undefined ? (
      <p className="muted">{empty}</p>
    ) : (
      <p>{String(value)}</p>
    );
  const values = customerValues(value);
  if (!values.length) return <p className="muted">{empty}</p>;
  return (
    <dl className="result-grid">
      {values.map((item, index) => (
        <div key={index}>
          <dt>
            {t(locale).resultDetail} {index + 1}
          </dt>
          <dd>{String(item)}</dd>
        </div>
      ))}
    </dl>
  );
}
function ValueModelExplanation({
  model,
  locale,
  expanded = false,
}: {
  model: WorkItem["valueModel"];
  locale: Locale;
  expanded?: boolean;
}) {
  if (!model) return null;
  const c = t(locale);
  const explanation =
    typeof model.explanation === "object"
      ? localized(model.explanation, locale)
      : model.explanation;
  return (
    <details className="value-explanation" open={expanded}>
      <summary>{c.valueHelp}</summary>
      {explanation && <p>{explanation}</p>}
      <div className="formula">
        <span>{formulaLabel(model.formula, locale)}</span>
        {model.baselineSeconds !== undefined &&
          model.loadedHourlyCost !== undefined && (
            <strong>
              {Math.round(model.baselineSeconds / 60)} min ×{" "}
              {money(model.loadedHourlyCost, model.currency || "EUR", locale)}
              {c.perHour}
            </strong>
          )}
      </div>
      {model.assumptions !== undefined && model.assumptions !== null && (
        <div className="value-assumptions">
          <strong>{c.assumptions}</strong>
          <PlainValue
            value={model.assumptions}
            empty={c.noDetails}
            locale={locale}
          />
        </div>
      )}
    </details>
  );
}

function ArtifactList({
  token,
  artifacts,
  locale,
}: {
  token: string;
  artifacts: WorkArtifact[];
  locale: Locale;
}) {
  const c = t(locale);
  return (
    <div className="artifact-list">
      {artifacts.map((artifact) => (
        <button
          key={`${artifact.artifactId}-${artifact.role}`}
          onClick={() => void downloadArtifact(token, artifact)}
        >
          <span>▤</span>
          <div>
            <strong>
              {typeof artifact.metadata?.name === "string"
                ? artifact.metadata.name
                : c.artifact}
            </strong>
            <small>
              {artifact.mimeType}
              {artifact.sizeBytes ? ` · ${artifact.sizeBytes} ${c.bytes}` : ""}
            </small>
          </div>
          <span>{c.download} ↓</span>
        </button>
      ))}
    </div>
  );
}
function eventLabel(event: TimelineEvent, locale: Locale): string {
  const labels: Record<string, [string, string]> = {
    work_item_created: ["Work item created", "Item de trabalho criado"],
    attempt_created: ["Attempt created", "Tentativa criada"],
    node_started: ["Work started", "Trabalho iniciado"],
    node_completed: [
      String(event.params.summary || "Business step completed"),
      "Etapa de negócio concluída",
    ],
    work_blocked: ["Waiting for a person", "A aguardar uma pessoa"],
    blocker_resolved: ["Response received", "Resposta recebida"],
    work_completed: ["Work completed", "Trabalho concluído"],
    work_failed: ["Could not complete", "Não foi possível concluir"],
    feedback_recorded: ["Feedback captured", "Feedback registado"],
    revision_requested: ["Revised attempt requested", "Nova tentativa pedida"],
    value_credited: ["Estimated value recorded", "Valor estimado registado"],
    value_deducted: ["Estimated value deducted", "Valor estimado deduzido"],
    consequence_confirmation_required: [
      "Confirmation required",
      "Confirmação necessária",
    ],
    consequence_repetition_confirmed: [
      "Repeated actions confirmed",
      "Ações repetidas confirmadas",
    ],
  };
  return (labels[event.code] || ["Work updated", "Trabalho atualizado"])[
    locale === "pt" ? 1 : 0
  ];
}
function Timeline({
  events,
  nodes,
  locale,
}: {
  events: TimelineEvent[];
  nodes: DetailData["nodes"];
  locale: Locale;
}) {
  const nodeMap = new Map(nodes.map((node) => [node.id, node]));
  return (
    <ol className="timeline">
      {events.map((event) => (
        <li key={event.id}>
          <i
            className={
              event.code.includes("failed")
                ? "failed"
                : event.code.includes("blocked")
                  ? "blocked"
                  : ""
            }
          />
          <div>
            <strong>{eventLabel(event, locale)}</strong>
            {event.nodeExecutionId && nodeMap.get(event.nodeExecutionId) && (
              <span>
                {localized(
                  nodeMap.get(event.nodeExecutionId)?.businessLabel,
                  locale,
                )}
              </span>
            )}
            <time>{when(event.createdAt, locale)}</time>
          </div>
        </li>
      ))}
    </ol>
  );
}

function schemaFields(
  schema: JSONSchema,
): Array<[string, NonNullable<JSONSchema["properties"]>[string]]> {
  return Object.entries(schema.properties || {});
}

type ParsedField = { present: boolean; valid: boolean; value?: unknown };
function parseSchemaField(field: JSONSchema, draft: unknown): ParsedField {
  if (draft === undefined || draft === "")
    return { present: false, valid: true };
  if (field.enum) {
    const index = Number(draft);
    return Number.isInteger(index) && index >= 0 && index < field.enum.length
      ? { present: true, valid: true, value: field.enum[index] }
      : { present: true, valid: false };
  }
  if (field.type === "boolean")
    return { present: true, valid: typeof draft === "boolean", value: draft };
  if (field.type === "number" || field.type === "integer") {
    const value = Number(draft);
    return {
      present: true,
      valid:
        Number.isFinite(value) &&
        (field.type !== "integer" || Number.isInteger(value)),
      value,
    };
  }
  if (field.type === "object" || field.type === "array") {
    try {
      const value = JSON.parse(String(draft)) as unknown;
      const valid =
        field.type === "array"
          ? Array.isArray(value)
          : !!value && typeof value === "object" && !Array.isArray(value);
      return { present: true, valid, value };
    } catch {
      return { present: true, valid: false };
    }
  }
  return { present: true, valid: true, value: String(draft) };
}
function schemaFieldError(field: JSONSchema, locale: Locale): string {
  const c = t(locale);
  if (field.type === "integer") return c.invalidInteger;
  if (field.type === "number") return c.invalidNumber;
  if (field.type === "array") return c.invalidArray;
  return c.invalidObject;
}
function BlockerForm({
  token,
  blocker,
  locale,
  onDone,
}: {
  token: string;
  blocker: Blocker;
  locale: Locale;
  onDone: () => Promise<void>;
}) {
  const c = t(locale);
  const [drafts, setDrafts] = useState<Record<string, unknown>>({});
  const [outcome, setOutcome] = useState(
    blocker.allowedOutcomes.length === 1 ? blocker.allowedOutcomes[0] : "",
  );
  const [confirmed, setConfirmed] = useState<string[]>([]);
  const [file, setFile] = useState<File | null>(null);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState("");
  const fields = schemaFields(blocker.responseSchema);
  const presentationFields = new Map(
    (blocker.responsePresentation?.fields || []).map((field) => [
      field.key,
      field,
    ]),
  );
  const outcomeLabels = blocker.responsePresentation?.outcomes || [];
  const required = blocker.responseSchema.required || [];
  const parsed = new Map(
    fields.map(([key, field]) => [key, parseSchemaField(field, drafts[key])]),
  );
  const response = Object.fromEntries(
    fields.flatMap(([key]) => {
      const value = parsed.get(key);
      return value?.present && value.valid ? [[key, value.value]] : [];
    }),
  );
  const consequences = blocker.requiredConsequences || [];
  const exactConsequencesConfirmed =
    blocker.kind !== "consequence_confirmation" ||
    (confirmed.length === consequences.length &&
      consequences.every((entry) => confirmed.includes(entry.invocationId)));
  const valid =
    !!outcome &&
    required.every(
      (key) => parsed.get(key)?.present && parsed.get(key)?.valid,
    ) &&
    [...parsed.values()].every((value) => !value.present || value.valid) &&
    (blocker.kind !== "artifact" || !!file) &&
    exactConsequencesConfirmed;
  async function submit(event: FormEvent) {
    event.preventDefault();
    if (!valid) return;
    setBusy(true);
    try {
      const refs: Array<{ artifactId: string; metadata?: unknown }> = [];
      if (file) {
        const artifact = await uploadArtifact(token, file);
        refs.push({ artifactId: artifact.id, metadata: { name: file.name } });
      }
      await respondToBlocker(
        token,
        blocker,
        outcome,
        response,
        refs,
        confirmed,
      );
      await onDone();
    } catch (err) {
      setError(err instanceof Error ? err.message : "Error");
    } finally {
      setBusy(false);
    }
  }
  return (
    <section className="blocker-box">
      <span className="eyebrow">{blockerKindLabel(blocker.kind, locale)}</span>
      <h3>{localized(blocker.title, locale)}</h3>
      <p>{localized(blocker.description, locale)}</p>
      <form onSubmit={(event) => void submit(event)}>
        {blocker.allowedOutcomes.length > 1 && (
          <label>
            {c.blockerOutcome}
            <select
              value={outcome}
              onChange={(event) => setOutcome(event.target.value)}
            >
              <option value="">{c.chooseOutcome}</option>
              {blocker.allowedOutcomes.map((option, index) => (
                <option value={option} key={option}>
                  {localized(outcomeLabels[index], locale) ||
                    `${c.choice} ${index + 1}`}
                </option>
              ))}
            </select>
          </label>
        )}
        {fields.map(([key, field], fieldIndex) => {
          const fieldState = parsed.get(key);
          const fieldPresentation = presentationFields.get(key);
          return (
            <label key={key}>
              {localized(fieldPresentation?.label, locale) ||
                `${c.responseField} ${fieldIndex + 1}`}
              {field.enum ? (
                <select
                  value={String(drafts[key] ?? "")}
                  onChange={(event) =>
                    setDrafts((current) => ({
                      ...current,
                      [key]: event.target.value,
                    }))
                  }
                >
                  <option value="" />
                  {field.enum.map((option, index) => (
                    <option value={index} key={JSON.stringify(option)}>
                      {localized(fieldPresentation?.options?.[index], locale) ||
                        `${c.choice} ${index + 1}`}
                    </option>
                  ))}
                </select>
              ) : field.type === "boolean" ? (
                <input
                  type="checkbox"
                  checked={drafts[key] === true}
                  onChange={(event) =>
                    setDrafts((current) => ({
                      ...current,
                      [key]: event.target.checked,
                    }))
                  }
                />
              ) : (
                <textarea
                  inputMode={
                    field.type === "number" || field.type === "integer"
                      ? "decimal"
                      : undefined
                  }
                  value={String(drafts[key] ?? "")}
                  onChange={(event) =>
                    setDrafts((current) => ({
                      ...current,
                      [key]: event.target.value,
                    }))
                  }
                />
              )}
              {fieldState?.present && !fieldState.valid && (
                <span className="field-error" role="alert">
                  {schemaFieldError(field, locale)}
                </span>
              )}
            </label>
          );
        })}
        {blocker.kind === "artifact" && (
          <label>
            {c.uploadArtifact}
            <input
              type="file"
              onChange={(event) => setFile(event.target.files?.[0] || null)}
            />
          </label>
        )}
        {blocker.kind === "consequence_confirmation" &&
          consequences.length > 0 && (
            <div className="consequence-box">
              <strong>△ {c.consequences}</strong>
              <p>{c.blockerConsequenceWarning}</p>
              {consequences.map((consequence) => (
                <label key={consequence.invocationId}>
                  <input
                    type="checkbox"
                    checked={confirmed.includes(consequence.invocationId)}
                    onChange={(event) =>
                      setConfirmed((current) =>
                        event.target.checked
                          ? [...current, consequence.invocationId]
                          : current.filter(
                              (id) => id !== consequence.invocationId,
                            ),
                      )
                    }
                  />
                  <span>{localized(consequence.summary, locale)}</span>
                </label>
              ))}
            </div>
          )}
        {error && <div className="form-error">{error}</div>}
        <button className="button primary" disabled={!valid || busy}>
          {c.submitResponse}
        </button>
      </form>
    </section>
  );
}

function ValueModal({
  locale,
  data,
  close,
}: {
  locale: Locale;
  data: NonNullable<ReturnType<typeof useDashboardData>["data"]>;
  close: () => void;
}) {
  const c = t(locale);
  const model = data.summary.value.models[0];
  const total = data.summary.value.totals[0];
  return (
    <div
      className="modal-layer"
      onMouseDown={(event) => {
        if (event.target === event.currentTarget) close();
      }}
    >
      <div className="modal value-modal" role="dialog" aria-modal="true">
        <button
          className="icon-button modal-close"
          aria-label={c.close}
          onClick={close}
        >
          <CloseIcon />
        </button>
        <span className="eyebrow">{c.valueHelp}</span>
        <h2>
          {data.summary.value.configured && total
            ? money(total.amount, total.currency, locale)
            : c.notConfigured}
        </h2>
        {model ? (
          <ValueModelExplanation model={model} locale={locale} expanded />
        ) : (
          <p>{c.unconfiguredBody}</p>
        )}
      </div>
    </div>
  );
}

export default function App() {
  const [locale, setLocale] = useState<Locale>("en");
  const [token, setToken] = useState<string | null>(null);
  const [connectError, setConnectError] = useState("");
  const [connectLoading, setConnectLoading] = useState(false);
  const [preset, setPreset] = useState("week");
  const [customFrom, setCustomFrom] = useState("");
  const [customTo, setCustomTo] = useState("");
  const [search, setSearch] = useState("");
  const [selected, setSelected] = useState<WorkItem | null>(null);
  const [valueOpen, setValueOpen] = useState(false);
  const period = useMemo(
    () =>
      preset === "custom" && customFrom && customTo
        ? {
            from: new Date(`${customFrom}T00:00:00`),
            to: new Date(`${customTo}T23:59:59.999`),
          }
        : presetPeriod(preset),
    [preset, customFrom, customTo],
  );
  const { data, error, loading, refresh } = useDashboardData(
    token,
    period,
    search,
  );
  async function connect(value: string) {
    setConnectError("");
    setConnectLoading(true);
    try {
      await loadDashboard(value, period, search);
      setToken(value);
    } catch {
      setConnectError(t(locale).invalidKey);
    } finally {
      setConnectLoading(false);
    }
  }
  if (!token)
    return (
      <Connection
        locale={locale}
        setLocale={setLocale}
        onConnect={connect}
        loading={connectLoading}
        error={connectError}
      />
    );
  const c = t(locale);
  const items = data?.items || [];
  return (
    <div className="app-shell">
      <Header
        locale={locale}
        setLocale={setLocale}
        search={search}
        setSearch={setSearch}
        items={items}
        disconnect={() => {
          setToken(null);
          setSelected(null);
        }}
      />
      <main className="dashboard-main">
        <ValueHero
          data={data}
          locale={locale}
          onExplain={() => setValueOpen(true)}
        />
        <section className="controls">
          <div>
            <span className="eyebrow">{c.date}</span>
            <select
              aria-label={c.date}
              value={preset}
              onChange={(event) => setPreset(event.target.value)}
            >
              <option value="today">{c.today}</option>
              <option value="week">{c.week}</option>
              <option value="month">{c.month}</option>
              <option value="custom">{c.custom}</option>
            </select>
            {preset === "custom" && (
              <>
                <label>
                  <span className="sr-only">{c.from}</span>
                  <input
                    type="date"
                    value={customFrom}
                    onChange={(event) => setCustomFrom(event.target.value)}
                  />
                </label>
                <label>
                  <span className="sr-only">{c.to}</span>
                  <input
                    type="date"
                    value={customTo}
                    onChange={(event) => setCustomTo(event.target.value)}
                  />
                </label>
              </>
            )}
          </div>
          <span>
            {items.length} {c.itemsShown}
          </span>
        </section>
        {error && (
          <div className="page-error" role="alert">
            {c.refreshError}
          </div>
        )}
        {loading && !data ? (
          <div className="loading page-loading">{c.loading}</div>
        ) : (
          <Board items={items} locale={locale} open={setSelected} />
        )}
      </main>
      {selected && (
        <DetailPanel
          token={token}
          item={selected}
          locale={locale}
          close={() => setSelected(null)}
          changed={() => void refresh(true)}
        />
      )}{" "}
      {valueOpen && data && (
        <ValueModal
          locale={locale}
          data={data}
          close={() => setValueOpen(false)}
        />
      )}
    </div>
  );
}
