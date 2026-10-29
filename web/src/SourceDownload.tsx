import React, { useEffect, useState } from 'react';
import { Download, PackageCheck } from 'lucide-react';
import './source-download.css';

type Row = Record<string, any>;
const size = (n: number) => (n < 1024 * 1024 ? Math.ceil(n / 1024) + ' KB' : (n / 1024 / 1024).toFixed(1) + ' MB');

/** Download of the source exactly as accepted; only for completed objectives. */
export function SourceDownload({ goal }: { goal: Row }) {
  const [source, setSource] = useState<Row | null>(null);
  const [error, setError] = useState('');
  useEffect(() => {
    if (goal.state !== 'completed') return;
    let live = true;
    fetch('/api/objectives/' + goal.id + '/source')
      .then(async (r) => {
        const d = await r.json();
        if (!r.ok) throw Error(d.message ?? 'Download indisponível.');
        if (live) setSource(d);
      })
      .catch((e) => live && setError(e.message));
    return () => {
      live = false;
    };
  }, [goal.id, goal.state]);
  if (goal.state !== 'completed') return null;
  return (
    <section className="source-download" aria-label="Baixar sistema entregue">
      <PackageCheck size={26} />
      <div>
        <h3>Sistema entregue</h3>
        {source ? (
          <p>
            {source.fileCount} arquivos · {size(source.totalBytes)} · versão aceita em{' '}
            {new Date(source.acceptedAt).toLocaleString('pt-BR')} ({source.taskKey})
            <code title={'SHA256 da versão aceita: ' + source.candidateDigest}>
              {source.candidateDigest.slice(0, 12)}
            </code>
          </p>
        ) : (
          <p role={error ? 'alert' : 'status'}>{error || 'Conferindo a versão aceita…'}</p>
        )}
      </div>
      {source && (
        <a className="button primary" href={'/api/objectives/' + goal.id + '/source.zip'} download={source.fileName}>
          <Download size={17} /> Baixar sistema (.zip)
        </a>
      )}
    </section>
  );
}
