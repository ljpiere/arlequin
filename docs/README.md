# Documentación

- `architecture.md`: diagrama de alto nivel (Mermaid) de los componentes y flujo de datos en Arlequin.
- `evaluation.md`: guía breve de cómo interpretar p-valores y δ, y cómo reproducir tablas de comparación E1/E2/E3.

## Generar los diagramas

Los diagramas están en formato Mermaid para facilitar su edición en Markdown.
Puedes visualizarlos directamente en editores compatibles o usar herramientas
como `mmdc` (mermaid-cli) si deseas exportarlos a PNG/SVG.

```bash
# opcional
npm install -g @mermaid-js/mermaid-cli
mmdc -i docs/architecture.md -o docs/architecture.svg
```

