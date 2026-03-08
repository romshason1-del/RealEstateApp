export type UserRole = "basic" | "pro";

export type Profile = {
  id: string;
  role: UserRole;
  full_name: string | null;
  created_at: string;
  updated_at: string;
};
