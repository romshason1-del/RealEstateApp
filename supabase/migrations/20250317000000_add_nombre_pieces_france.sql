-- Add nombre_pieces_principales to properties_france for room count display
-- DVF column 39: Nombre pieces principales

alter table public.properties_france
  add column if not exists nombre_pieces_principales integer;
